open Prelude
open Printf
open ExtLib

let log = Log.from "main"

(* adapted from Parallel.run_workers *)
let run_workers workers (type t) (type u) (f : t -> u) (g : u -> unit) enum =
  assert (workers > 0);
  let module Worker = struct type task = t type result = u end in
  let module W = Parallel.Forks(Worker) in
  let worker x =
    (* sane signal handler FIXME restore? *)
    Signal.set_exit Daemon.signal_exit;
    f x
  in
  let proc = W.create worker workers in
  Nix.handle_sig_exit_with ~exit:true (fun () -> W.stop proc); (* FIXME: output in signal handler *)
  W.perform ~autoexit:true proc enum g;
  W.stop proc;
  ()

exception Success

module type WorkerT = sig
  type task
  type result
end

module type Workers = sig
  type task
  type result
  type t
  val create : (task Lwt_stream.t -> result Lwt_stream.t) -> int -> t
  val perform : t -> ?autoexit:bool -> task Lwt_stream.t -> result Lwt_stream.t
  val stop : ?wait:int -> t -> unit Lwt.t
end

module LwtStreamForks(T: WorkerT) = struct

type task = T.task

type result = T.result

type instance = { mutable ch : (Lwt_io.input_channel * Lwt_io.output_channel) option; pid : int; }

type t = {
  mutable running : instance list;
  execute : (task Lwt_stream.t -> result Lwt_stream.t);
  mutable alive : bool;
  mutable gone : int;
}

let exn_lwt_suppress f x = try%lwt f x with _ -> Lwt.return_unit (* TODO move to Exn_lwt *)

let input_value ?on_exn input =
  match%lwt Lwt_io.read_value input with
  | exception End_of_file -> Lwt.return_none
  | exception exn ->
    log #info ~exn "input_value";
    let%lwt () = match on_exn with Some on_exn -> on_exn exn | None -> Lwt.return_unit in
    Lwt.return_none
  | v ->
    Lwt.return_some v

let output_value output v =
  let%lwt () = Lwt_io.write_value output v in
  let%lwt () = Lwt_io.flush output in
  Lwt.return_unit

let input_stream ?on_exn input = Lwt_stream.from (fun () -> input_value ?on_exn input)

let output_stream output stream = Lwt_stream.iter_s (output_value output) stream

let worker (execute : task Lwt_stream.t -> result Lwt_stream.t) =
  let (main_read, child_write) = Unix.pipe () in
  let (child_read, main_write) = Unix.pipe () in
  match Nix.fork () with
  | `Child ->
    Unix.close main_read; Unix.close main_write;
    Unix.set_close_on_exec child_read;
    Unix.set_close_on_exec child_write;
    let output = Lwt_io.of_unix_fd ~mode:Lwt_io.Output child_write in
    let input = Lwt_io.of_unix_fd ~mode:Lwt_io.Input child_read in
    let (get_task_stream, push) = Lwt_stream.create () in
    let result_stream =
      let on_exn exn = log #error ~exn "Parallel.worker failed to unmarshal task"; Lwt.return_unit in
      let input_stream = input_stream ~on_exn input in
      Lwt_stream.from begin fun () ->
        push (Some `GetTask);
        Lwt_stream.get input_stream
      end |>
      execute |>
      Lwt_stream.map (fun x -> `Result x)
    in
    let result_stream =
      Lwt_stream.append result_stream @@
      Lwt_stream.from (fun () -> push None; Lwt.return_none)
    in
    Lwt_main.run begin
      let%lwt () =
        Lwt_stream.choose [ get_task_stream; result_stream; ] |>
        output_stream output
      in
      let%lwt () = exn_lwt_suppress Lwt_io.close input in
      let%lwt () = exn_lwt_suppress Lwt_io.close output in
      Lwt.return_unit
    end;
    exit 0
  | `Forked pid ->
    Unix.close child_read; Unix.close child_write;
    (* prevent sharing these pipes with other children *)
    Unix.set_close_on_exec main_write;
    Unix.set_close_on_exec main_read;
    let cout = Lwt_io.of_unix_fd ~mode:Lwt_io.Output main_write in
    let cin = Lwt_io.of_unix_fd ~mode:Lwt_io.Input main_read in
    { ch = Some (cin, cout); pid; }

let create execute n =
  let running = List.init n (fun _ -> worker execute) in
  { running; execute; alive = true; gone = 0; }

let close_ch w =
  match w.ch with
  | Some (cin, cout) ->
    w.ch <- None;
    let%lwt () = exn_lwt_suppress Lwt_io.close cin in
    let%lwt () = exn_lwt_suppress Lwt_io.close cout in
    Lwt.return_unit
  | None ->
    Lwt.return_unit

(** @return list of reaped and live pids *)
let reap l =
  let open Unix in
  List.partition (fun pid ->
  try
    pid = fst (waitpid [WNOHANG] pid)
  with
  | Unix_error (ECHILD,_,_) -> true (* exited *)
  | exn -> log #warn ~exn "Worker PID %d lost (wait)" pid; true) l

let hard_kill l =
  let open Unix in
  let (_, live) = reap l in
  live |> List.iter begin fun pid ->
    try
      kill pid Sys.sigkill; log #warn "Worker PID %d killed with SIGKILL" pid
    with
    | Unix_error (ESRCH,_,_) -> ()
    | exn -> log #warn ~exn "Worker PID %d (SIGKILL)" pid end

let killall signo pids =
  pids |> List.iter begin fun pid ->
    try Unix.kill pid signo with exn -> log #warn ~exn "PID %d lost (trying to send signal %d)" pid signo
  end

let do_stop ?wait pids =
  let rec reap_loop timeout l =
    let (_,live) = reap l in
    match timeout, live with
    | _, [] -> Lwt.return `Done
    | Some 0, l -> hard_kill l; Lwt.return (`Killed (List.length l))
    | _, l -> let%lwt () = Lwt_unix.sleep 1. in reap_loop (Option.map pred timeout) l
  in
  killall Sys.sigterm pids;
  reap_loop wait pids

let stop ?wait t =
  let gone () = if t.gone = 0 then "" else sprintf " (%d workers vanished)" t.gone in
  log #info "Stopping %d workers%s" (List.length t.running) (gone ());
  t.alive <- false;
  let%lwt l = t.running |> Lwt_list.map_p (fun w -> let%lwt () = close_ch w in Lwt.return w.pid) in
  let%lwt () = Lwt_unix.sleep 0.1 in (* let idle workers detect EOF and exit peacefully (frequent io-in-signal-handler deadlock problem) *)
  t.running <- [];
  match%lwt do_stop ?wait l with
  | `Done -> log #info "Stopped %d workers properly%s" (List.length l) (gone ()); Lwt.return_unit
  | `Killed killed -> log #info "Timeouted, killing %d (of %d) workers with SIGKILL%s" killed (List.length l) (gone ()); Lwt.return_unit

let perform t ?(autoexit=false) tasks =
  let stream =
    match t.running with
    | [] -> [ t.execute tasks; ] (* no workers *)
    | _ ->
    let lock = Lwt_mutex.create () in
    t.running |>
    List.filter_map begin fun w ->
      match w.ch with
      | None -> None
      | Some (cin, cout) ->
      let on_exn exn =
        log #warn ~exn "no result from PID %d" w.pid;
        t.gone <- t.gone + 1;
        (* close pipes and forget dead child, do not reap zombie so that premature exit is visible in process list *)
        let%lwt () = close_ch w in
        t.running <- List.filter (fun w' -> w'.pid <> w.pid) t.running;
        Lwt.return_unit
      in
      let output () =
        match%lwt Lwt_mutex.with_lock lock (fun () -> Lwt_stream.get tasks) with
        | Some x -> output_value cout x
        | None -> exn_lwt_suppress Lwt_io.close cout
      in
      let input = input_stream ~on_exn cin in
      let rec next () =
        match%lwt Lwt_stream.get input with
        | Some `GetTask ->
          let%lwt () = output () in
          next ()
        | Some (`Result x) ->
          Lwt.return_some x
        | None ->
          let%lwt () = exn_lwt_suppress Lwt_io.close cin in
          let%lwt () = close_ch w in
          Lwt.return_none
      in
      Some (Lwt_stream.from next)
    end
  in
  let finish =
    Lwt_stream.from begin fun () ->
      begin match t.gone with
      | 0 -> log #info "Finished"
      | n -> log #warn "Finished, %d workers vanished" n
      end;
      let%lwt () = stop t in
      Lwt.return_none
    end
  in
  Lwt_stream.append (Lwt_stream.choose stream) finish

end

let run_workers_lwt_stream workers (type t) (type u) (f : t Lwt_stream.t -> u Lwt_stream.t) (stream : t Lwt_stream.t) =
  assert (workers > 0);
  let module Worker = struct type task = t type result = u end in
  let module W = LwtStreamForks(Worker) in
  let worker x =
    (* sane signal handler FIXME restore? *)
    Signal.set_exit Daemon.signal_exit;
    f x
  in
  let proc = W.create worker workers in
  Nix.handle_sig_exit_with ~exit:true (fun () -> Lwt.async (fun () -> W.stop proc)); (* FIXME: output in signal handler *)
  W.perform ~autoexit:true proc stream

let () =
  print_endline "start 10 workers";
  run_workers 10 (printf "start %d\n") id (List.enum (List.init 100 id));
  Lwt_main.run (Lwt_main.yield ());
  let test_workers nr =
    let stream =
      Lwt_stream.append (Lwt_stream.of_list [ "first line"; ]) (Lwt_stream.of_list (List.init 100 (sprintf "line %d")))
    in
    printf "start %d workers\n" nr;
    run_workers_lwt_stream nr begin fun stream ->
      print_endline "start lwt worker";
      let stream =
        Lwt_stream.map_s begin fun line ->
          let%lwt () = Lwt_unix.sleep 1. in
          let pid = Unix.getpid () in
          printf "%s pid %d\n" line pid;
          flush stdout;
          Lwt.return (sprintf "return %s from pid %d" line pid)
        end stream
      in
      print_endline "stop lwt worker";
      stream
    end stream |>
    Lwt_stream.iter print_endline
  in
  Lwt_main.run (test_workers 10);
  Lwt_main.run (test_workers 20)
