---------------------------- MODULE CronJobController ----------------------------
\* Repeated syncs of one CronJob (pkg/controller/cronjob). The schedule fires at
\* every integer instant 1..MaxTime (abstracted: the step model handles the
\* missed-count arithmetic). The controller is stateless between syncs: it reads
\* the persisted status (active, lst), so a restart or downtime is any stretch of
\* Ticks without Sync. A status write may fail; job creations and deletions made
\* in that sync stay.
EXTENDS Integers, FiniteSets

CONSTANTS MaxTime,      \* last instant
          Policy,       \* "Allow" | "Forbid" | "Replace"
          HasDeadline,  \* startingDeadlineSeconds set
          Deadline,     \* its value in ticks
          HistoryZero,  \* TRUE: successful/failedJobsHistoryLimit = 0 (finished jobs deleted)
          AllowSuspend, \* TRUE: suspend may be toggled
          WritesCanFail \* TRUE: UpdateStatus may fail after jobs were created/deleted

Times == 1..MaxTime

VARIABLES now, lst, active, exists, running, lingering, created, late, suspend

vars == <<now, lst, active, exists, running, lingering, created, late, suspend>>

Init == /\ now = 0
        /\ lst = 0               \* creationTimestamp = 0, no lastScheduleTime
        /\ active = {}           \* persisted status.active (by scheduled time = job name)
        /\ exists = {}           \* Job objects in the API
        /\ running = {}          \* Jobs not finished
        /\ lingering = {}        \* deleted by Replace, pods still terminating (background propagation)
        /\ created = [t \in Times |-> 0]   \* successful Creates per scheduled time
        /\ late = -1             \* largest now - scheduled at a creation
        /\ suspend = FALSE

Tick == /\ now < MaxTime
        /\ now' = now + 1
        /\ UNCHANGED <<lst, active, exists, running, lingering, created, late, suspend>>

Finish(t) == /\ t \in running
             /\ running' = running \ {t}
             /\ UNCHANGED <<now, lst, active, exists, lingering, created, late, suspend>>

PodsGone(t) == /\ t \in lingering
               /\ lingering' = lingering \ {t}
               /\ UNCHANGED <<now, lst, active, exists, running, created, late, suspend>>

Toggle == /\ AllowSuspend
          /\ suspend' = ~suspend
          /\ UNCHANGED <<now, lst, active, exists, running, lingering, created, late>>

Max(a, b) == IF a > b THEN a ELSE b

Earliest == IF ~HasDeadline THEN lst ELSE Max(lst, now - Deadline)
Unmet == {t \in Times : Earliest < t /\ t <= now}
Sched == CHOOSE t \in Unmet : \A u \in Unmet : u <= t

\* sync(): cleanupFinishedJobs, then syncCronJob, then one UpdateStatus (ok or lost).
Sync(ok) ==
  LET finished == exists \ running
      ex1 == IF HistoryZero THEN exists \ finished ELSE exists
      \* reconciliation: refs to finished or missing jobs dropped; running jobs
      \* not in active ("UnexpectedJob") are NOT added.
      a1 == active \cap running
      none == suspend \/ Unmet = {}
      t == IF none THEN 0 ELSE Sched
      skip == none \/ t \in a1 \/ (Policy = "Forbid" /\ a1 # {})
      repl == ~skip /\ Policy = "Replace"
      a2 == IF repl THEN {} ELSE a1
      ex2 == IF repl THEN ex1 \ a1 ELSE ex1
      fresh == ~skip /\ t \notin ex2
      ex3 == IF fresh THEN ex2 \cup {t} ELSE ex2
      a3 == IF skip THEN a2 ELSE a2 \cup {t}    \* created or adopted (AlreadyExists, owned, not in active)
      l3 == IF skip THEN lst ELSE t
  IN /\ exists' = ex3
     /\ running' = IF repl THEN (running \ a1) \cup (IF fresh THEN {t} ELSE {})
                   ELSE running \cup (IF fresh THEN {t} ELSE {})
     /\ lingering' = IF repl THEN lingering \cup a1 ELSE lingering
     /\ created' = IF fresh THEN [created EXCEPT ![t] = @ + 1] ELSE created
     /\ late' = IF fresh THEN Max(late, now - t) ELSE late
     /\ active' = IF ok THEN a3 ELSE active
     /\ lst' = IF ok THEN l3 ELSE lst
     /\ UNCHANGED <<now, suspend>>

Next == \/ Tick
        \/ \E t \in Times : Finish(t) \/ PodsGone(t)
        \/ Toggle
        \/ \E ok \in (IF WritesCanFail THEN BOOLEAN ELSE {TRUE}) : Sync(ok)

Spec == Init /\ [][Next]_vars

\* Properties
OnePerTime     == \A t \in Times : created[t] <= 1
ForbidSafe     == Policy = "Forbid" => Cardinality(running) <= 1
ReplaceJobs    == Policy = "Replace" => Cardinality(running) <= 1
ReplacePods    == Policy = "Replace" => Cardinality(running \cup lingering) <= 1
DeadlineOK     == ~HasDeadline \/ late < Deadline
=============================================================================
