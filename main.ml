open Prelude
open Printf
open ExtLib

let () =
  let result = Parallel.invoke ((^) "return ") "line 1" in
  let result = result () in
  print_endline result;
  let result =
    Parallel.invoke begin fun () ->
      let (stream, push) = Lwt_stream.create () in
      let thread =
        Thread.create begin fun () ->
          Lwt_main.run begin
            Lwt_stream.iter print_endline stream
          end
        end ()
      in
      Lwt_preemptive.run_in_main begin fun () ->
        push (Some "line 2");
        push None;
        Lwt.return_unit
      end;
      Thread.join thread;
      "return result"
    end ()
  in
  let result = result () in
  print_endline result
