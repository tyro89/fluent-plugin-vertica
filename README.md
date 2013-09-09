Fluentd output plugin for Vertica
=================================

Simple batched output plugin for getting events into vertica.

Example config
==============

    <match vertica.public.test>
      type vertica

      database mydb
      schema public
      table test

      username dbadmin
      password mypass

      host 127.0.0.1
      port 5433
    </match>
