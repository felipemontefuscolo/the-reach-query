import sys
import time
import json
import os
import numpy
from math import *
from collections import OrderedDict
import names


#########################################################
############## Change parameters here ###################

# You usually want to change one parameter: max_n_chars. All other parameters are deducted from that

n_users = 100000 #2* 26L*(1 - 26**max_n_chars)//(1-26)
window_size = 10000 # dimensionless time: number of requests. Must be multiple of 5
n_tweets = 4L*n_users # WARNING: it needs to be multiple of n_users
n_unique_tweets = 50 # chance of retweet: 1 - (1.*(n_unique_tweets-1)/n_unique_tweets)**(followers_mean*n_tweets/n_users)
followers_mean = 40 #500/(1 + exp(-1e-5*(n_users-350000))) # average is limited to 500 followers
followers_dev = 40 #0.5*abs(n_users/4 - followers_mean)

main_folder = "/home/ubuntu/db/test02"

#########################################################

def window_path(x): return main_folder + "/window" + format(x, '05')
def create_window_path(x):
    w_path = main_folder
    try:
        os.makedirs(w_path)
    except:
        pass
    return w_path

    #// my operator are        schema
    #//                        code id:short
    #// * add new user     |   1              id:Long ts:Long name:string
    #// * delete user      |   2              id:Long ts:Long name:string
    #// * follow           |   3              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * unfollow         |   4              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * new tweet        |   5              id:Long ts:Long msg:string  user_id:Long

print ""
print "generating graph with:"
print "number of users: ", n_users
print "average number of followers: ", followers_mean
print "deviation number of followers: ", followers_dev
print ""

w_path = create_window_path(0)
os.system("rm -f " + w_path + "/*") # CLEAN
f = open(w_path + "/everything.txt", "w")
f_tweet = open(w_path + "/tweets_only.txt", "w")

print ("creating users ...")
ts = 0L
# USERS
for i in range(0L, n_users):
    json_str = json.dumps({"code":"user", "id":i, "ts":ts, "name":names.get_first_name()+" "+names.get_last_name()})
    f.write(json_str + "\n")
    ts += 1

print("creaing follows ...")
# FOLLOWS
f_count = 0
for i in range(0L, n_users):
    n_followers = min( long(numpy.random.normal(followers_mean, followers_dev)) , n_users)
    for kk in range(0, n_followers):
        follower = numpy.random.randint(0L,n_users)
        if follower == i:
            continue
        json_str = json.dumps({"code":"follow","id":f_count,"ts":ts,"follower_id":follower,"followed_id":i})
        f_count += 1
        ts += 1
        f.write(json_str + "\n")

# TWEETS
#with open('dictionary.json') as raw_tt:
#    tt = json.load(raw_tt)
#tt = list( k+": "+v for k,v in tt.items() )
print("creating tweets ...")
for i in range(0L, n_tweets):
    user = numpy.random.randint(0,n_users)
    msg = user % n_unique_tweets
    msg = "Engineered data number = " + str(msg)
    json_str = json.dumps({"code":"tweet","id":i,"ts":ts,"msg":msg,"user_id":user})
    f.write(json_str + "\n")
    f_tweet.write(json_str + "\n")
    ts += 1
 

f.close()
f_tweet.close()

l = len(str(ts // window_size))

os.system("cd " + main_folder + "; split --additional-suffix=.dat -a " + str(l) + " -d -l " + str(window_size) + " " + w_path + "/everything.txt")
#os.system("rm -f " + w_path + "/everything.txt")

os.system("cd " + main_folder + "; split --additional-suffix=.dat -a " + str(l) + " -d -l " + str(window_size) + " " + w_path + "/tweets_only.txt t")
#os.system("rm -f " + w_path + "/tweets_only.txt")

print("Number of relationships " + str(f_count))
