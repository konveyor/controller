# controller
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fkonveyor%2Fcontroller.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fkonveyor%2Fcontroller?ref=badge_shield)

Common controller lib.  Provides components shared by application controllers.

Requires: Go 1.13+ and Go Modules

---
**Logging**

Logging can be configured using environment variables:
- LOG_DEVELOPMENT: Development mode with human readable logs and (default) verbosity=4.
- LOG_LEVEL: Set the verbosity.

Verbosity:
- Info(3) used for `Info` logging.
- Info(4) used for `Debug` logging.
- Info(5) used for `Debug+` high rate events.

Package:
- filebacked:
  - Info(5): file create,delete
  - Info(6): file read,write.
- inventory:
  - container:
    - Info(3): reconciler lifecycle.
    - Error(4): channel send failed.
  - model:
    - Info(3):
      - database: lifecycle.
      - journal: journal and watch lifecycle.
      - model: insert,update,delete.
    - Info(4):
      - client: (db) transaction lifecycle;model get,list.
      - journal: event staging.
      - watch: lifecycle.
    - Info(5):
      - watch: event sent,received.
      - table: SQL statements.
  - web:
    - Info(3):
      - watch: lifecycle.
    - Info(4):
      - watch: event sent,received. 
  - ref:
    - Info(3): _reference_ mapping added,deleted.
    - Info(4): _reference_ lookup and reconcile events queued.

---



## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fkonveyor%2Fcontroller.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fkonveyor%2Fcontroller?ref=badge_large)