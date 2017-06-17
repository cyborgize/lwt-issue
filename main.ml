open Prelude
open Printf
open ExtLib

let () =
  let result = Parallel.invoke ((^) "return ") "line 1" in
  let result = result () in
  print_endline result;
  let result =
    Parallel.invoke begin fun () ->
      let (waiter, wakener) = Lwt.wait () in
      let thread =
        Thread.create begin fun () ->
          Lwt_main.run waiter
        end ()
      in
      Lwt_preemptive.run_in_main begin fun () ->
        Lwt.wakeup wakener ();
        Lwt.return_unit
      end;
      Thread.join thread;
      "return result"
    end ()
  in
  let result = result () in
  print_endline result
