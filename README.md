# Twitter Analyzing

Java example of analyzing twitter data with hadoop map reduce.

 - `TwitterFollowerCounter` - evaluate number of followers per user
 - `TwitterAvgFollowers` - evaluate average number of followers per all users
 - `TwitterTopFollowers` - evaluate to 50 users by number of followers
 - `TwitterFollowerCounterGroupByRanges` - evaluate number of followers in ranges [1 .. 10], [11, 100], [101, 1000], ...

As you can see in [build.log](https://github.com/pahaz/twitter-hadoop-example/blob/master/build.log.txt) and [result.log](https://github.com/pahaz/twitter-hadoop-example/blob/master/result.log.txt), the most followed person is user 16409683 - @britneyspears (according to 2011).

# Source Data format
`/data/twitter/twitter_rv.net` contains strings like `user_id \t follower_id` (without spaces around \t)

# How-to

    git clone https://github.com/pahaz/twitter-hadoop-example.git
    cd twitter-hadoop-example
    
    mvn compile
    mvn package
    
    hadoop fs -rm -r /user/s0073/count || echo "No"
    hadoop fs -rm -r /user/s0073/avg || echo "No"
    hadoop fs -rm -r /user/s0073/top || echo "No"
    hadoop fs -rm -r /user/s0073/range || echo "No"
    
    hadoop jar target/hhd-1.0-SNAPSHOT.jar TwitterFollowerCounter /data/twitter/twitter_rv.net /user/s0073/count
    hadoop jar target/hhd-1.0-SNAPSHOT.jar TwitterAvgFollowers /user/s0073/count /user/s0073/avg
    hadoop jar target/hhd-1.0-SNAPSHOT.jar TwitterTopFollowers /user/s0073/count /user/s0073/top
    hadoop jar target/hhd-1.0-SNAPSHOT.jar TwitterFollowerCounterGroupByRanges /user/s0073/count /user/s0073/range

## File formats

 - `/user/s0073/count` - contains strings like `user_id \t number_of_followers` (without spaces around \t)
 - `/user/s0073/avg` - contains one string `0 \t average_number_of_followers` (without spaces around \t, 0 is fictive key)
 - `/user/s0073/top` - contains 50 strings like `0 \t user_id \t number_of_followers` (without spaces around \t, 0 is fictive key)
 - `/user/s0073/range` - contains strings like `range_number \t number_of_users` (without spaces around \t, range_number example: 10 - [1 .. 10], 11 - [11 .. 100], ...)

## File cat

    # hadoop fs -cat /user/s0073/count/*
    hadoop fs -cat /user/s0073/avg/*
    hadoop fs -cat /user/s0073/top/*
    hadoop fs -cat /user/s0073/range/*

## Other

Sometime you need up hadoop job priority. Use this command:

    mapred job -set-priority <job_id> VERY_HIGH
