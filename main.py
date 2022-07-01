import concurrent.futures
import os
from typing import Dict, Any, List

from fuzzywuzzy import fuzz

allFields = ['first_name', 'last_name', 'email', 'class_year', 'date_of_birth']


def find_duplicates(**kwargs):
    profiles: List[Dict[str, Any]] = kwargs.get("profiles")
    fields: List[str] = kwargs.get("fields")
    futures: List[concurrent.futures.Future] = []
    results = []
    # Using a thread pool to speed up the process [Speed/Performance]
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        for i in range(len(profiles)):
            for j in range(len(profiles)):
                if i < j:
                    futures.append(executor.submit(check_duplicate, profiles[i], profiles[j], fields))
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
        executor.shutdown(wait=True)
    return results


def check_duplicate(profile1: Dict[str, Any], profile2: Dict[str, Any], fields: List[str]):
    fieldSet = set(fields)
    matching_attributes = set()
    non_matching_attributes = set()
    totalScore = 0

    # Add fields for basic equality check here... [Extensibility]
    for field in set(fields).intersection(['class_year', 'date_of_birth']):
        if field in profile1 or field in profile2:
            if field in profile1 and field in profile2 \
                    and profile1[field] and profile2[field]:
                if profile1[field] == profile2[field]:
                    totalScore += 1
                    matching_attributes.add(field)
                else:
                    totalScore -= 1
        fieldSet.remove(field)
        if totalScore > 1:
            break

    if totalScore <= 1:
        # We can write more chunks of code here to contribute to duplicate finding [Extensibility]
        if 'first_name' in fieldSet or 'last_name' in fieldSet or 'email' in fieldSet \
                and fuzz.ratio(profile1['first_name'] + profile1['last_name'] + profile1['email'],
                               profile2['first_name'] + profile2['last_name'] + profile2['email']) > 80:
            totalScore += 1
            for field in {'first_name', 'last_name', 'email'}.intersection(fields):
                if profile1[field] == profile2[field]:
                    matching_attributes.add(field)
    ignored_attributes = set(allFields).difference(set(matching_attributes).union(set(non_matching_attributes)))
    return "Profile %s" % profile1['id'], \
           "Profile %s" % profile2['id'], \
           totalScore, \
           list(matching_attributes), \
           list(non_matching_attributes), \
           list(ignored_attributes)


if __name__ == '__main__':
    duplicates = find_duplicates(
        profiles=[{
            'id': 1,
            'email': 'knowkanhai@gmail.com',
            'first_name': 'Kanhai',
            'last_name': 'Shah',
            'class_year': None,
            'date_of_birth': None
        },
            {
                'id': 2,
                'email': 'knowkanhai@gmail.com',
                'first_name': 'Kanhai',
                'last_name': 'Shah',
                'class_year': 2012,
                'date_of_birth': '1990-10-11'
            }, {
                'id': 3,
                'email': 'knowkanhai@gmail.com',
                'first_name': 'Kanhai',
                'last_name': 'Shah',
                'class_year': None,
                'date_of_birth': None
            },
            {
                'id': 4,
                'email': 'knowkanhai@gmail.com',
                'first_name': 'Kanhai',
                'last_name': 'Shah',
                'class_year': 2012,
                'date_of_birth': '1990-10-11'
            }],
        fields=['email', 'first_name', 'last_name', 'class_year', 'date_of_birth']
    )
    for dup in duplicates:
        print(dup)

    # ------------------ Sample 3 --------------------

    # print(check_duplicate(
    #     {
    #         'id': 1,
    #         'email': 'knowkanhai@gmail.com',
    #         'first_name': 'Kanhai',
    #         'last_name': 'Shah',
    #         'class_year': 2012,
    #         'date_of_birth': '1990-10-11'
    #     },
    #     {
    #         'id': 2,
    #         'email': 'knowkanhai@gmail.com',
    #         'first_name': 'Kanhai',
    #         'last_name': 'Shah',
    #         'class_year': 2012,
    #         'date_of_birth': '1990-10-11'
    #     },
    #     ['first_name', 'last_name']
    # ))

    # ------------------ Sample 3 --------------------
