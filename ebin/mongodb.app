{application, mongodb,
 [{description, "Mongodb Client"},
  {vsn, "0.0.1"},
  {modules, [
             mongodb_wire
            ]},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {registered, []},
  {env, [
         %% Set default timeout for operations.
         %% Individual operation timeouts can be supplied,
         %% e.g. get_timeout, put_timeout that will 
         %% override the default.
         {timeout, 60000}
        ]}
 ]}.

