# rtask - Redis backed background task processing

We decided to build a custom job processing library for the following reasons:

* Most of the work is already done by Redis. We only need simple very simple instrumentation on top of the existing features.
* Ability to perform tasks without using Redis. This is very useful for testing.
* Custom events and logging, for monitoring memory and performance.
