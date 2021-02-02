#! /usr/bin/env python
import json
import re
import csv
import sys


def convert_adm_to_csv(file_name: str):
    headers = ['id', 'create_at', 'text', 'in_reply_to_status', 'in_reply_to_user', 'favorite_count', 'retweet_count',
               'lang', 'is_retweet', 'hashtags', 'user_mentions', "user_id", "user_name", "user_screen_name",
               "user_location", "user_description", "user_followers_count", "user_friends_count", "user_statues_count",
               "stateName", "countyName",
               "cityName", "country", "bounding_box"]
    with open(f"{file_name}.csv", 'w') as output_file:
        w = csv.writer(output_file)
        first = True
        with open(file_name, 'r') as file:
            for i, line in enumerate(file):
                line = re.sub(r"(datetime\()(\"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\dZ\")(\))", r"\2", line)
                line = re.sub(r"(date\()(\"\d\d\d\d-\d\d-\d\d\")(\))", r"\2", line)
                line = re.sub(r"(rectangle\()(\"-?\d*\.\d*,-?\d*\.\d* -?\d*\.\d*,-?\d*\.\d*\")(\))", r"\2", line)
                line = re.sub(r"(point\()(\"-?\d*\.\d*,-?\d*\.\d*\")(\))", r"\2", line)
                line = re.sub(r"({{)", r"[", line)
                line = re.sub(r"(}})", r"]", line)
                if first:
                    w.writerow(headers)
                    first = False
                res = []
                payload = json.loads(line.strip())
                for header in headers:

                    if header in ["bounding_box", "country"] and payload.get("place"):
                        value = payload.get("place").get(header)
                    elif header in ["stateName", "countyName", "cityName"] and payload.get("geo_tag"):
                        value = payload.get("geo_tag").get(header)
                    elif header in ["user_id", "user_name", "user_screen_name", "user_location", "user_description",
                                    "user_followers_count", "user_friends_count", "user_statues_count"] \
                            and payload.get("user"):
                        value = payload.get("user").get(header[5:])
                    elif header in 'is_retweet':
                        value = int(payload.get(header))
                    else:
                        value = json.loads(line.strip()).get(header)
                    res.append(value)
                w.writerow(res)


if __name__ == '__main__':
    convert_adm_to_csv(sys.argv[1])
