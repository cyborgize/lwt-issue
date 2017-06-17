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
    let%lwt () = match on_exn with Some on_exn -> on_exn exn | None -> Lwt.return_unit in
    Lwt.return_none
  | v ->
    Lwt.return_some v

let input_stream ?on_exn input =
  Lwt_stream.from (fun () -> input_value ?on_exn input)

let output_value output v =
  let%lwt () = Lwt_io.write_value output v in
  let%lwt () = Lwt_io.flush output in
  Lwt.return_unit

let output_stream output stream = Lwt_stream.iter_s (output_value output) stream

let worker (execute : task Lwt_stream.t -> result Lwt_stream.t) =
  let main_read, child_write = Unix.pipe () in
  let child_read, main_write = Unix.pipe () in
  match Nix.fork () with
  | `Child -> (* child *)
      Unix.close main_read; Unix.close main_write;
      Unix.set_close_on_exec child_read;
      Unix.set_close_on_exec child_write;
      Lwt_main.run begin
        let output = Lwt_io.of_unix_fd ~mode:Lwt_io.Output child_write in
        let input = Lwt_io.of_unix_fd ~mode:Lwt_io.Input child_read in
        let%lwt () =
          let on_exn exn = log #error ~exn "Parallel.worker failed to unmarshal task"; Lwt.return_unit in
          input_stream ~on_exn input |>
          execute |>
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
  let streams =
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
      let output =
        let rec loop () =
          match%lwt Lwt_mutex.with_lock lock (fun () -> Lwt_stream.get tasks) with
          | Some x -> let%lwt () = output_value cout x in loop ()
          | None -> Lwt.return_unit
        in
        loop ()
      in
      let input =
        let input = input_stream ~on_exn cin in
        Lwt_stream.from begin fun () ->
          match%lwt Lwt_stream.get input with
          | Some _ as x -> Lwt.return x
          | None ->
            Lwt.cancel output;
            Lwt.return_none
        end
      in
      Some input
    end
  in
  let stream = Lwt_stream.choose streams in
  Lwt_stream.from begin fun () ->
    match%lwt Lwt_stream.get stream with
    | Some _ as answer -> Lwt.return answer
    | None ->
    begin match t.gone with
    | 0 -> log #info "Finished"
    | n -> log #warn "Finished, %d workers vanished" n
    end;
    let%lwt () = stop t in
    Lwt.return_none
  end

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

let run_lwt_workers workers (type t) (f : t Lwt_stream.t -> unit Lwt.t) enum =
  let enum = Enum.append (Enum.map (fun x -> Some x) enum) (Enum.init workers (fun _ -> None)) in
  let thread = ref None in
  let (stream, push) = Lwt_stream.create_bounded 1 in
  let worker event =
    print_endline "start worker";
    Lwt_main.run begin
      print_endline "start lwt_main_run";
      let%lwt () = f stream in
      print_endline "stop lwt_main_run";
      Lwt.return_unit
    end;
    print_endline "stop worker"
  in
  let worker = function
    | Some x ->
      if !thread = None then begin
        print_endline "create thread";
        thread := Some (Thread.create worker ());
      end;
      print_endline "call run_in_main";
      Lwt_preemptive.run_in_main begin fun () ->
        print_endline "start run_in_main";
        let%lwt () = Lwt_unix.sleep 0.1 in
        let%lwt () = Lwt.pick [ push #push x; Daemon.wait_exit (); ] in
        print_endline "stop run_in_main";
        Lwt.return_unit
      end;
      print_endline "called run_in_main"
    | None ->
      print_endline "close stream";
      begin match !thread with
      | None -> ()
      | Some thread ->
        Lwt_preemptive.run_in_main begin fun () ->
          push #close;
          Lwt.return_unit
        end;
        print_endline "join thread";
        Thread.join thread
      end;
      print_endline "raise success";
      raise Success
  in
  run_workers workers worker id enum

let () =
  (*
  print_endline "start 10 workers";
  run_workers 10 (printf "start %d\n") id (List.enum (List.init 100 id));
 *)
(*
  match Lwt_unix.fork () with
  | 0 -> print_endline "in child"; exit 0
  | _ ->
*)
  print_endline "create enum";
  let _enum = Enum.append (List.enum [ "first line" ]) (Enum.init 100 (sprintf "line %d")) in
  let stream =
    Lwt_stream.append (Lwt_stream.of_list [ "first line"; ]) (Lwt_stream.of_list (List.init 100 (sprintf "line %d")))
  in
  print_endline "start 1 worker";
(*
  run_workers 1 print_endline id enum;
*)
(*
  run_lwt_workers 1 begin fun stream ->
    print_endline "start lwt worker";
    let%lwt () = Lwt_stream.iter print_endline stream in
    print_endline "stop lwt worker";
    Lwt.return_unit
  end enum
*)
  Lwt_main.run begin
    let stream =
      run_workers_lwt_stream 1 begin fun stream ->
        print_endline "start lwt worker";
        let stream =
          Lwt_stream.map begin fun line ->
            print_endline line;
            sprintf "return %s" line
          end stream
        in
        print_endline "stop lwt worker";
        stream
      end stream
    in
    Lwt_stream.iter print_endline stream
  end
