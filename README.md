Hello
=====
This is a vary good simple project to demonstrate a concurrency feature of Go programming language.
Modify from online training course by Mike Van Sickle from pluralsight.com https://app.pluralsight.com/library/courses/go-concurrent-programming/table-of-contents

How to use?
===========
Please Remark first section of code to test the traditional pattern that will complete job within 18-20 sec.
Then swap to the first section and see the performance for your self.

Notice: On the line 183 and 202 there are a sleep time to simulate i/o and service call delay as in real life transaction.
If we remove this line performance will be very fast because it use map object to deal with transform(), this is a real power of go!
As you can see in optimize go routine code it still have delay time simulated but the performance is not effect from i/o or service delay any more
