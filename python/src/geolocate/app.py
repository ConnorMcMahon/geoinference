##
#  Copyright (c) 2015, Derek Ruths, David Jurgens
#
#  All rights reserved. See LICENSE file for details
##

#type this line (assuming tcsh) before running:
#setenv LD_PRELOAD /usr/lib/x86_64-linux-gnu/libgfortran.so.3

import argparse
import json
import simplejson
import traceback
#import jsonlib
import logging
import os, os.path
import datetime
import gzip
import time

from collections import defaultdict
from gimethod import gimethod_subclasses, GIMethod
from dataset import Dataset, posts2dataset
from sparse_dataset import SparseDataset
from geopy.distance import vincenty
from geopy.distance import great_circle
from shuffle import random

logger = logging.getLogger(__name__)

NUM_MALE_FOLDS = 5
NUM_FEMALE_FOLDS = 5
NUM_UNKNOWN_FOLDS = 5

NUM_URBAN_FOLDS = 5

NUM_RANDOM_FOLDS = 5

FOLD_SIZE = 28000

def get_method_by_name(name):
    # get the geoinference class
    candidates = gimethod_subclasses(name)

    if len(candidates) == 0:
        logger.fatal('No geoinference named "%s" was found.' % name)
        logger.info('Available methods are: %s' % ','.join([x.__name__ for x in gimethod_subclasses("")]))
        quit()

    if len(candidates) > 1:
        logger.fatal('More than one geoinference method named "%s" was found.')
        quit()

    return candidates[0]

def ls_methods(args):
    """
    Print out the set of methods that the tool knows about.
    """
    for x in gimethod_subclasses():
        print('\t' + x.__name__)

def generate_folds(l, numfolds):
    shuffle(l)
    x = l[0:FOLD_SIZE]
    size_per_fold = FOLD_SIZE / numfolds
    currentIndex = 0
    folds = []
    for i in range(numfolds):
        folds[i] = x[0:currentIndex] + x[currentIndex + size_per_fold : FOLD_SIZE]
        currentIndex += size_per_fold
    return folds

def write_fold(fold, currentFold, idToLoc, output_user_ids_file_handles):
    for user_id in fold:
        loc = idToLoc[user_id]
        output_user_ids_file_handles[currentFold].write("%s\t%s\t%s" % (user_id, loc[0], loc[1]))

def create_folds(args): 
    parser = argparse.ArgumentParser(prog='geoinf create_folds', description='creates a set of data partitions for evaluating with cross-fold validation')
    parser.add_argument('-f', '--force', help='overwrite the output model directory if it already exists')
    parser.add_argument('dataset_dir', help='a directory containing a geoinference dataset')
    parser.add_argument('fold_dir', help='a (non-existent) directory that will contain the information on the cross-validation folds')
    parser.add_argument('test_case', help="What type of test wanted to run i.e. rural vs urban (county), gender (gender), or random (any other string)")

    args = parser.parse_args(args)

    # Confirm that the output directory doesn't exist
    if not os.path.exists(args.fold_dir): #and not args.force:
        #raise Exception, 'output fold_dir cannot already exist'
                os.mkdir(args.fold_dir)
    ground_truth_file = "filtered_user_groundtruth_locfield.tsv"
    ground_truth_locs = "users.home-locations.loc-field.tsv.gz"

    # Decide on the number of folds
    if num_folds <= 1:
        #raise Exception, 'The number of folds must be at least two'
        print("the number of folds must be at least two")

    if args.test_case == "gender":
        num_folds = NUM_MALE_FOLDS + NUM_FEMALE_FOLDS + NUM_UNKNOWN_FOLDS
    elif args.test_case == "county":
        num_folds = NUM_URBAN_FOLDS * 6
    else:
        num_folds = NUM_RANDOM_FOLDS

    idToGender = {}
    idToUrbanLevel = {}
    with open(os.path.join(args.dataset_dir, ground_truth_file), "r") as gt_file:
        gt_file.next();
        for line in gt_file:
            try:
                uid, gender, urbanLevel = line.split('\t')
                idToGender[uid] = gender
                if urbanLevel != "\r\n":
                    idToUrbanLevel[uid] = int(urbanLevel)
            except:
                print line

    idToLoc = {}
    with gzip.open(os.path.join(args.dataset_dir, ground_truth_locs), "r") as gt_file:
        gt_file.next()
        for line in gt_file:
            uid, lat, lon = line.split('\t')
            idToLoc[uid] = (lat, lon)

    # Initialize the output streams.  Rather than keeping things in memory,
    # we batch the gold standard posts by users (one at a time) and then
    # stream the user's gold standard posts (if any) to the output streams
    output_user_ids_file_handles = []
    cf_info_fh = open(os.path.join(args.fold_dir, "folds.info.tsv"), 'w')

    for i in range(0, num_folds):
        fold_name = "fold_%d" % i

        # All the IDs of the users with gold posts are written here
        fold_users_ids_fh = open(os.path.join(args.fold_dir, fold_name + ".user-ids.txt"), 'w')
        output_user_ids_file_handles.append(fold_users_ids_file_handles)

        cf_info_fh.write("%s\t%s.user-ids.txt" 
                                 % (fold_name, fold_name))
    cf_info_fh.close()

    # Load the dataset
    ds = SparseDataset(args.dataset_dir)

    if args.test_case == "gender":
        female_users = []
        male_users = []
        unknown_users = []

        for user in ds.user_iter():                             
            user_id = user['user_id']
            usergender = idToGender.get(str(user_id), -1)
            # If this user had any gold locations, add them as folds
            if usergender != -1:
                #determine fold to use
                if userGender == "m":
                    male_users.append(user_id)
                elif userGender == "f":
                    female_users.append(user_id)
                else:
                    unknown_users.append(user_id)
        currentFold = 0

        male_folds = generate_folds(male_users, NUM_MALE_FOLDS)
        for fold in male_folds:
            write_fold(fold, currentFold, idToLoc, output_user_ids_file_handles)
            currentFold += 1
        
        female_folds = generate_folds(female_users, NUM_FEMALE_FOLDS)
        for fold in female_folds:
            write_fold(fold, currentFold, idToLoc, output_user_ids_file_handles)
            currentFold += 1
        
        unknown_folds = generate_folds(unknown_users, NUM_UNKNOWN_FOLDS)
        for fold in unknown_folds:
            write_fold(fold, currentFold, idToLoc, output_user_ids_file_handles)
            currentFold += 1
    elif args.test_case == "county":
        usersAtLevel = []
        for i in range(1, 7):
            usersAtLevel[i] = []
        for user in ds.user_iter():
            user_id = user['user_id']
            urbanRuralLevel = idToUrbanLevel.get(str(user,id) -1)
            # If this user had any gold locations, add them as folds
            if urbanRuralLevel != -1:
                usersAtLevel[urbanRuralLevel].append(user_id)
        currentFoldIndex = 0
        for i in range(1,7):        
            currentFolds = generate_folds(usersAtLevel[i], NUM_URBAN_FOLDS)
            for fold in currentFolds:
                write_fold(fold, currentFoldIndex, idToLoc, output_users_ids_file_handles)
                currentFoldIndex += 1
    else:
        # Iterate over the dataset looking for posts with geo IDs that we can
        # use as a gold standard
        for user in ds.user_iter():
            gold_users = []
            user_id = user['user_id']
            gender = idToGender.get(str(user_id), -1)
            # If this user had any gold locations, add them as folds
            if gender != -1:
                gold_users.append(uid)
                
        currentFoldIndex = 0
        currentFolds = generate_folds(gold_users, NUM_RANDOM_FOLDS)
        for fold in currentFolds:
            write_fold(fold, currentFoldIndex, idToLoc, output_users_ids_file_handles)
            currentFoldIndex += 1
            
    for fh in output_user_ids_file_handles:
        fh.close()

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def cross_validate(args): 
    parser = argparse.ArgumentParser(prog='geoinf cross_validate', description='evaluate a geocinference method using cross-validation')
    parser.add_argument('-f', '--force', help='overwrite the output model directory if it already exists')
    parser.add_argument('method_name', help='the method to use')
    parser.add_argument('method_settings', help='a json file containing method-specific configurations')
    parser.add_argument('dataset_dir', help='a directory containing a geoinference dataset')
    parser.add_argument('fold_dir', help='the name of the directory containing information on the cross-validation folds')
    parser.add_argument('results_dir', help='a (non-existent) directory where the evaluation results will be stored')
    parser.add_argument('--fold', nargs=1, 
                        help='runs just that fold from the cross-fold dataset')
    parser.add_argument('--location-source', nargs=1, 
                        help='specifies the source of ground-truth locations')

    args = parser.parse_args(args)

    # confirm that the output directory doesn't exist
#   if os.path.exists(args.results_dir) and not args.force:
#       raise Exception, 'output results_dir cannot already exist'

    if not os.path.exists(args.results_dir): #and not args.force:
        #raise Exception, 'output fold_dir cannot already exist'
        os.mkdir(args.results_dir)
    ground_truth_file = "sample_dataset/users.home-locations.loc-field.tsv.gz"

    # load the method
    method = get_method_by_name(args.method_name)

    gold_location = {}
    with gzip.open(ground_truth_file, 'r') as fh:
        fh.next()
        for line in fh:
            user_id, lat, lon = line.split('\t')
            gold_location[user_id] = (float(lat), float(lon))


    # load the data
    with open(args.method_settings, 'r') as fh:
        settings = json.load(fh)

        specific_fold_to_run = args.fold
        if specific_fold_to_run:
            specific_fold_to_run = specific_fold_to_run[0]
        location_source = args.location_source
        if location_source:
            print('Using %s as the source of ground truth location' % location_source)
            location_source = location_source[0]
            settings['location_source'] = location_source

                
        print("running fold %s" % (specific_fold_to_run))

    # Load the folds to be used in the dataset
    cfv_fh = open(os.path.join(args.fold_dir, 'folds.info.tsv'))

    # Each line contains two files specifying the post IDs to be held out
    # from the full dataset (for that fold) and the corresponding file in
    # the fold_dir containing the testing data for that fold
    for line in cfv_fh:
        line = line.strip()
        fold_name, location_source = line.split("\t")

        # Skip this fold if the user has told us to run only one fold by name
        if specific_fold_to_run is not None and fold_name != specific_fold_to_run:
            continue

        logger.debug('starting processing of fold %s' % fold_name)
                
        # Read in the post IDs to exclude
        testing_post_ids = set()
        tpi_fh = open(os.path.join(args.fold_dir, testing_post_ids_file.replace('held-out-','')))
        for id_str in tpi_fh:
            testing_post_ids.add(id_str.strip())
        tpi_fh.close()

        # Read in the user IDs to exclude
        testing_user_ids = set()
        tpi_fh = open(os.path.join(args.fold_dir, testing_user_ids_file.replace('held-out-','')))
        for id_str in tpi_fh:
            testing_user_ids.add(id_str.strip())
        tpi_fh.close()

        logger.debug('Loaded %d users whose location data will be held out' % len(testing_user_ids))

        # load the dataset
        training_data = None
        if not location_source is None:
            training_data = SparseDataset(args.dataset_dir, args.fold_dir, default_location_source=location_source)
        else:
            training_data = SparseDataset(args.dataset_dir, args.fold_dir)
                
        # load the method
        method = get_method_by_name(args.method_name)
        method_inst = method()
        
        # Create the temporary directory that will hold the model for
        # this fold
        model_dir = os.path.join(args.results_dir, fold_name)
        if not os.path.exists(model_dir):
            os.mkdir(model_dir)
                
        # Train on the datset, holding out the testing post IDs
        started, finished = method_inst.train_model(settings, training_data, None)

        #print('Finished training during fold %s; beginning testing' % fold_name)

        #print("Reading testing data from %s" % (os.path.join(args.fold_dir,testing_users_file)))

        #testing_data = Dataset(args.fold_dir, users_file=os.path.join(args.fold_dir,testing_users_file))

        print("Writing results to %s" % (os.path.join(args.results_dir, fold_name + ".results.tsv.gz")))
                
        out_fh = gzip.open(os.path.join(args.results_dir, fold_name + ".results.tsv.gz"), 'w')

        initial_users = set(started.keys())
        final_users = set(finished.keys())
        gold_standard_users = set(gold_location.keys())
        predicted_users = final_users - initial_users
        test_users = predicted_users & gold_standard_users
        print("Found %d new users after starting with %d users" % (len(predicted_users), len(initial_users)))
        print("Reporting results of %d users with known locations and predicted locations" % (len(test_users)))

        out_fh.write("%s\t%s\t%s\t%s\t%s\t%s\n" % ("user_id", "known_lat", "known_lon", "pred_lat", "pred_lon", "distance (km)"))
        for user in test_users:
            #print('%s\t%s\t%s\t%s\t%s\n' % (user, gold_location[user][0], gold_location[user][1].strip(), finished[user][1], finished[user][0]))
            prevMedian = (gold_location[user][0], gold_location[user][1])
            testMedian = finished[user]
            try:
                distance = vincenty(prevMedian, testMedian).kilometers
            except:
                distance = great_circle(prevMedian, testMedian).kilometers
            out_fh.write('%s\t%s\t%s\t%s\t%s\t%d\n' % (user, gold_location[user][0], gold_location[user][1], finished[user][0], finished[user][1], distance))

        for user in (predicted_users - gold_standard_users):
            out_fh.write('%s\t%s\t%s\t%s\t%s\t%s\n' % (user, "none", "none", finished[user][0], finished[user][1], "none"))

        out_fh.close()
        
        """
        num_tested_users = 0
        num_tested_posts = 0
        seen_ids = set()
        for user in testing_data.user_iter():
            user_id = user['user_id']
            posts = user['posts']

            locs = model.infer_posts_locations_by_user(user_id, posts)

            if len(locs) != len(posts):
                print("#WUT %d != %d" % (len(locs), len(posts)))
                        
                num_located_posts = 0 
                num_tested_posts += len(posts)
            for loc, post in zip(locs, posts):
                pid = post['id']
                if pid in seen_ids:
                        continue
                seen_ids.add(pid)
                if not loc is None:
                    out_fh.write('%s\t%f\t%f\n' % (post['id'], loc[0], loc[1]))
                    num_located_posts += 1
                    num_tested_users += 1
                    if num_tested_users % 10000 == 0:
                        print('During testing of fold %s, processed %d users, %d posts, %d located' % (fold_name, num_tested_users, num_tested_posts, num_located_posts))

        out_fh.close()
        """
        print('Finished testing of fold %s' % fold_name)



def train(args):
    parser = argparse.ArgumentParser(prog='geoinf train',description='train a geoinference method on a specific dataset')
    parser.add_argument('-f','--force',help='overwrite the output model directory if it already exists')
    parser.add_argument('method_name',help='the method to use')
    parser.add_argument('method_settings',help='a json file containing method-specific configurations')
    parser.add_argument('dataset_dir',help='a directory containing a geoinference dataset')
    parser.add_argument('model_dir',help='a (non-existing) directory where the trained model will be stored')
    parser.add_argument('--location-source', nargs=1, 
                        help='specifies the source of ground-truth locations')
        
    args = parser.parse_args(args)

    # confirm that the output directory doesn't exist
    if os.path.exists(args.model_dir) and not args.force:
        #raise Exception, 'output model_dir cannot exist'
        print("output model_dir cannot exist")

    # load the method
    method = get_method_by_name(args.method_name)

    # load the data
    with open(args.method_settings,'r') as fh:
        settings = json.load(fh)

        location_source = args.location_source
        if location_source:
            location_source = location_source[0]
            logger.debug('Using %s as the source of ground truth location'
                         % location_source)
            settings['location_source'] = location_source



    # load the dataset
    ds = None #Dataset(args.dataset_dir)
    if not location_source is None:
            ds = SparseDataset(args.dataset_dir, default_location_source=location_source)
    else:
            ds = SparseDataset(args.dataset_dir)


    # load the method
    method = get_method_by_name(args.method_name)
    method_inst = method()

    start_time = time.time()       
    method_inst.train_model(settings,ds,args.model_dir)
    end_time = time.time()
    logger.info('Trained model %s on dataset %s in %f seconds' 
                    % (args.method_name, args.dataset_dir, end_time - start_time))

    # drop some metadata into the run method
    # run the method
    # gi_inst = method()
    # gi_inst.train(settings,ds,args.model_dir)

    return

def infer(args,by_user=False):
    prog_name = 'geoinf'
    if by_user:
        description='infer the location of posts in a dataset using a specific inference method. Posts will be provided to the method grouped by user.'
        prog_name += ' infer_by_user'
    else:
        description='infer the location of posts in a dataset using a specific inference method. Posts will be provided to the method one at a time.'
        prog_name += ' infer_by_post'

    parser = argparse.ArgumentParser(prog=prog_name,description=description)
    parser.add_argument('-f','--force',action='store_true',help='overwrite the output file if it already exists')
    parser.add_argument('-s','--settings',help='a json file of settings to be passed to the model',nargs=1)
    parser.add_argument('method_name',help='the type of method to use for inference')
    parser.add_argument('model_dir',help='the directory of a model that was constructed using the train procedure')
    parser.add_argument('dataset',help='a json specification for the dataset to infer locations on')
    parser.add_argument('infer_file',help='the file that the inferences will be written to')
        
    logger.debug('infer args = %s' % str(args))
    args = parser.parse_args(args)

    # load the infer settings if necessary
    settings = {}
    if args.settings:
        with open(args.settings,'r') as fh:
            settings = json.load(fh)
    
    if os.path.exists(args.infer_file) and not args.force:
        #raise Exception, 'output infer_file cannot exist'
        print("output infer_file cannot exist")

    # load the method
    method = get_method_by_name(args.method_name)
    method_inst = method()
    model = method_inst.load_model(args.model_dir,settings)

    # load the dataset
    ds = SparseDataset(args.dataset)

    # get the output file ready
    outfh = open(args.infer_file,'w')

    # write settings to the first line
    outfh.write('%s\n' % json.dumps({'method': args.method_name, 
                                     'settings': settings, 
                                     'dataset': args.dataset,
                                     'by_user': by_user}))
    
    # locate all the posts
    logger.info('inferring locations for posts')    
    if by_user:
        num_posts_seen = 0
        num_posts_located = 0
        num_users_seen = 0
        for user in ds.user_iter():
            user_id = user['user_id']
            posts = user['posts']

            locs = model.infer_posts_locations_by_user(user_id,posts)

            assert len(locs) == len(posts)
            num_users_seen += 1

            for loc,post in zip(locs,posts):
                num_posts_seen += 1
                if not loc is None:
                    num_posts_located += 1
                    outfh.write('%s\t%f\t%f\n' % (post['id'],loc[0],loc[1]))

                if num_posts_seen % 10000 == 0:
                        logger.debug("Saw %d users, %d posts, %d of which were located" % (num_users_seen, num_posts_seen, num_posts_located))
    else:
        num_posts_seen = 0
        num_posts_located = 0
        for post in ds.post_iter():
            user_id = post['user']['id_str']
            loc = model.infer_post_location(post)
            num_posts_seen += 1
            if not loc is None:
                outfh.write('%s\t%f\t%f\n' % (post['id'],loc[0],loc[1]))
                num_posts_located += 1
            if num_posts_seen % 10000 == 0:
                    logger.debug("Saw %d posts, %d of which were located" % (num_posts_seen, num_posts_located))

    outfh.close()

    # done

def get_uid_field(post):
    return post['user']['id_str']

def get_mention_users(post):
    return [mention['id_str'] for mention in post['entities']['user_mentions']]

def build_dataset(args):
    parser = argparse.ArgumentParser(prog='geoinf build_dataset',description='build a new dataset')
    parser.add_argument('-f','--force',action='store_true')
    parser.add_argument('dataset_dir',help='the directory to put the dataset in')
    parser.add_argument('posts_file',help='the posts.json.gz file to use')
    parser.add_argument('user_id_field',help='the field name holding the user id of the post author')
    parser.add_argument('mention_field',help='the field name holding the list of user ids mentioned in a post')

    args = parser.parse_args(args)

#   uid_field_name = args.user_id_field
    uid_field_name = args.user_id_field.split('.')[::-1]
    mention_field_name = args.mention_field.split('.')[::-1]
    posts2dataset(args.dataset_dir,args.posts_file,
                  get_uid_field,
                  get_mention_users,
                  force=args.force)
    
    # done

def main():
    parser = argparse.ArgumentParser(prog='geoinf',description='run a geolocation inference method on a dataset')
    parser.add_argument('-l','--log_level',
                        choices=['DEBUG','INFO','WARN','ERROR','FATAL'],
                        default='INFO',help='set the logging level')
    parser.add_argument('action',choices=['train','infer_by_post','infer_by_user',
                          'ls_methods','build_dataset','create_folds','cross_validate'],
            help='indicate whether to train a new model or infer locations')
    parser.add_argument('action_args',nargs=argparse.REMAINDER,
            help='arguments specific to the chosen action')

    args = parser.parse_args()

    logging.basicConfig(level=eval('logging.%s' % args.log_level),
                        format='%(message)s')

    try:
        if args.action == 'train':
            train(args.action_args)
        elif args.action == 'ls_methods':
            ls_methods(args.action_args)
        elif args.action == 'infer_by_post':
            infer(args.action_args,False)
        elif args.action == 'infer_by_user':
            infer(args.action_args,True)
        elif args.action == 'build_dataset':
            build_dataset(args.action_args)
        elif args.action == 'create_folds':
            create_folds(args.action_args)
        elif args.action == 'cross_validate':
            cross_validate(args.action_args)

        else:
            #raise Exception, 'unknown action: %s' % args.action
            print("Unknown action %s" % args.action)

    except Exception as error:
        traceback.print_exc()

    # done!

if __name__ == '__main__':
    main()


