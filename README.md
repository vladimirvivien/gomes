# Gomes
Gomes is a (100%) native Go framework API for the [Apache Mesos](http://mesos.apache.org/) cluster manager.  Gomes uses the HTTP wire protocol to send message and receive events to and from a running Mesos master.  This project provides an  idiomatic Go API that makes it super easy to create Mesos frameworks using Go. 

#### Gomes Example
See a simple Gomes test - https://github.com/vladimirvivien/gomes/blob/master/gomestest/gomestest.go

### What's Working
Gomes is at its (very early) infancy.  However, here is what the API can already do today:

* Register with running Master
* Receive Framework-Reisgered event
* Receive Framework-Reregistered event
* Receive Resource Offers event
* Receive Offer Rescinded event
* Receive Status Update
* ... and  more features to come out soon!

### Future Plans

* Gomes still needs tons of work to be done.
* There many pieces missing to get it stable and ready for usage.
* The goal is to get it on par to its C++ counterpart, libprocess.

### Background
Munch of the inspiration for this work came from:

* Ben Hindman Presentation at ApacheCon 2014 - https://www.youtube.com/watch?v=hTcZGODnyf0&list=FLrFKzp-3UzcAXfZy6DbkbHg
* And changes made to 0.19 version of Mesos.
* Email exchange and guidance from other Mesos members.