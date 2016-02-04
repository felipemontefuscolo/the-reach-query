import sys
import time
import json
import os
import numpy
from math import *
from collections import OrderedDict


#########################################################
############## Change parameters here ###################

# You usually want to change one parameter: max_n_chars. All other parameters are deducted from that

max_n_chars = 1 # maximum number of characters in user names. 4 will generator  formula:
n_users = 26L*(1 - 26**max_n_chars)//(1-26)
window_size = 5 # dimensionless time: number of requests. Must be multiple of 5
n_tweets = 4L*n_users
followers_mean = 500/(1 + exp(-1e-5*(n_users-350000))) # average is limited to 500 followers
followers_dev = 0.5*abs(n_users/4 - followers_mean)

main_folder = "/home/ubuntu/db/test01"

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
f = open(w_path + "/everything.txt", "w")

ts = 0L
# USERS
for i in range(0L, n_users):
    json_str = json.dumps({"code":"user", "id":i, "ts":ts, "name":str(i)})
    f.write(json_str + "\n")
    ts += 1

# FOLLOWS
f_count = 0
for i in range(0L, n_users):
    n_followers = min( long(numpy.random.normal(followers_mean, followers_dev)) , n_users)
    for kk in range(0, n_followers):
        follower = numpy.random.randint(0L,i+1)
        if follower == i:
            continue
        json_str = json.dumps({"code":"follow","id":f_count,"ts":ts,"follower_id":follower,"followed_id":i})
        f_count += 1
        ts += 1
        f.write(json_str + "\n")

# TWEETS
for i in range(0L, n_tweets):
    user = numpy.random.randint(0,n_users)
    msg = user % (user//3 + 1)
    json_str = json.dumps({"code":"tweet","id":i,"ts":ts,"msg":str(msg),"user_id":user})
    f.write(json_str + "\n")
    ts += 1
 

f.close()

l = len(str(ts // window_size))

os.system("cd " + main_folder + "; split --additional-suffix=.dat -a " + str(l) + " -d -l " + str(window_size) + " " + w_path + "/everything.txt")
os.system("rm -f " + w_path + "/everything.txt")


