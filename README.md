OnHosts | NotOnHosts
====================
A pair of MogileFS replication policies that ensure the files in the class it
is applied to are on the specified hosts, or not on the specified hosts.

A little hackier than I'd like due to some of the MogileFS internals, but
proven in production.

Our use case
------------
At Last.fm we have a large MogileFS cluster, mostly made up of machines with
hard disks, but some with SSD's. The machines with flash storage are capable of
serving vastly more traffic at a faster rate than the HD machines, so we want
to serve our most popular content from the SSD nodes.

* The "popular" files are put into a class with an OnHosts replication policy
to move them to the SSD machines.

* The less popular files are put into a class with a NotOnHosts replication
policy that ensures they are not on the SSD machines.

Configuration
-------------
Set the replication policy in the DB as usual, pass the hosts you wish to
target or avoid in the brackets after the policy. This is nasty, but I've not
had time to make it use a proper configuration yet.
