install.packages("twitteR")
library(twitteR)

consumer.key <- ""
consumer.secret <- ""
access.token <- ""
access.secret <- ""

setup_twitter_oauth(consumer.key, consumer.secret, access.token, access.secret)

tw = twitteR::searchTwitter('#tfl', n = 1e4, since = '2018-11-01', retryOnRateLimit = 1e3)
d = twitteR::twListToDF(tw)
write.table(d, file = "Tweets_tfl_4.csv",row.names=FALSE, na="", sep=",")

brexit = twitteR::searchTwitter('#brexit', n = 1e5, since = '2018-11-01', retryOnRateLimit = 1e3)
df_brexit = twitteR::twListToDF(brexit)
write.table(df_brexit, file = "Tweets_brexit_2.csv",row.names=FALSE, na="", sep=",")