# AI Usage Reflection

## My Role and Use of AI

I took the lead on this project and owned the final design choices,
integration, testing, and evaluation. I used ChatGPT to help translate
the design into code through boilerplate, implementation drafts,
technical explanations, and debugging support.

I built and ran the project locally and on EC2, checked the outputs,
compared configurations, and decided which suggestions to retain.
I remained responsible for understanding the implementation and
ensuring that the documentation reflected the actual results.

## Suggestions I Accepted

1. **A shared HTTP client:** I accepted connection reuse across sender
   threads to avoid creating a new client for every swipe.

2. **CSV export after ingest:** I accepted collecting measurements
   in memory and writing them afterward to keep disk writes outside
   the measured sending phase.

## Suggestions I Rejected or Corrected

1. **Switching to Apache HttpClient:** I chose to retain Java
   HttpClient because changing libraries was not required. The
   proposed wrapper also did not capture every HTTP replacement
   connection.

2. **Treating opening attempts as established connections:** I
   questioned the initial TCP analysis and extended it to classify
   HTTP and WebSocket traffic and check observed handshakes.
   I documented the remaining reconnection-count limitation.

## A Decision I Evaluated Myself

I selected the main-phase thread configuration using measured
results rather than assuming more threads would always be better.
I compared 32, 64, 128, and 256 threads and examined the difference
between repeated 256-thread runs.

The fastest observed run reached 6,293.81 main-phase swipes per
second. I retained the slower results as well and described 256
threads as the fastest observed configuration, rather than a
proven optimum.

## Validation

I compiled and deployed the application, ran the load tests,
checked swipe accounting and WebSocket updates, and reviewed the
CSV statistics and charts.

I also preserved negative push timestamp differences and explained
their clock-skew limitation instead of presenting them as valid
absolute network latency.
