To run Task 1:
Change the input path that's hard coded in ZipcodeTopology to get the output

Start storm by: start_storm_zoo.sh

Create JAR file: mvn package or IntelliJ Maven --> Package

submit to topology: storm jar target/Zipcode-1.0-SNAPSHOT.jar ZipcodeTopology PA2

The file should be printed in the area where it is supposed to 

To run Task 2:
Change the input path that's hard coded in ZipcodeTopology to get the output

Start storm by: start_storm_zoo.sh

Create JAR file: mvn package or IntelliJ Maven --> Package

submit to topology: storm jar target/Zipcode-1.0-SNAPSHOT.jar ZipcodeTopology PA2 -p

The file should be printed in the area where it is supposed to 

To run in the local machine:
add -l


Task 1 analysis:
Value of epsilon that was picked for this was 0.01. By making the value too big, 
it would create very small buckets where it stops considering  enough zipcodes and 
during the pruning process it stops considered valid ones that are needed to be 
counted to get the top 5 later down the line when getting newer regions. And if it's
too big, the pruning process would just end up being useless. 0.01 creates a bucket 
size of 100 values, which is enough to consider a lot of the frequency of the zipcode 
air quality values to be caught.

The value of the delta can be related to bucket size and pruning process. Here the bucket
size would be 100, so every time we get to the next bucket the value of delta increases. 
When delta increases it also affects the pruning process because any value of delta + frequency
that is less than the current bucket being process is removed.


