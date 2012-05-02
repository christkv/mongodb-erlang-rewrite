% Use these macros to write/read numbers from bson or mongo binary format
-define (put_int8u (N), (N):8/unsigned-little).
-define (put_int32u (N), (N):32/unsigned-little).
-define (put_int64u (N), (N):64/unsigned-little).

-define (get_int32u (N), N:32/unsigned-little).
-define (get_int64u (N), N:64/unsigned-little).
