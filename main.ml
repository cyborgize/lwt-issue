open Prelude
open Printf
open ExtLib

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
  let () = Lwt_main.run Lwt.return_unit in
  print_endline "start 10 workers";
  run_workers 10 (printf "start %d\n") id (List.enum (List.init 100 id));
(*
  match Lwt_unix.fork () with
  | 0 -> print_endline "in child"; exit 0
  | _ ->
*)
  print_endline "create enum";
  let enum = Enum.append (List.enum [ "first line" ]) (Enum.init 100 (sprintf "line %d")) in
  print_endline "start 1 worker";
(*
  run_workers 1 print_endline id enum;
*)
  run_lwt_workers 1 begin fun stream ->
    print_endline "start lwt worker";
    let%lwt () = Lwt_stream.iter print_endline stream in
    print_endline "stop lwt worker";
    Lwt.return_unit
  end enum
