import sys
import time
import json
from math import *

#########################################################
############## Change parameters here ###################

# You usually want to change one parameter: max_n_chars. All other parameters are deducted from that

max_n_chars = 4 # maximum number of characters in user names. 4 will generator  formula: 27*(1-27^N)/(1-27)
n_users = 27*(1 - 27**max_n_chars)//(1-27)
window_size = 10 # dimensionless time: each unit time is an operation for each of the five operations
n_tweets = 4*n_users
followers_mean = 500/(1 + exp(-1e-5*(n_users-350000))) # average is limited to 500 followers
followers_dev = 0.5*abs(n_users/4 - followers_mean)

main_folder = "/home/ubuntu/db/test01"

#########################################################

def path_new_users(x): return main_folder + "/window" + format(x, '010') + "new_users"
def path_del_users(x): return main_folder + "/window" + format(x, '010') + "del_users"
def path_follows(x):   return main_folder + "/window" + format(x, '010') + "follows"
def path_unfollows(x): return main_folder + "/window" + format(x, '010') + "unfollows"
def path_tweets(x):    return main_folder + "/window" + format(x, '010') + "tweets"   
def create_window(x):
    os.


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

u_count = 1L
t_count = 1L
ts = 0L  # fictitious time stamp

for w in range(1, n_windows):
     

