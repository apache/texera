from __future__ import print_function
from thirdparty.alchemyapi import AlchemyAPI
import json

alchemyapi = AlchemyAPI()

with open("promemail.txt", "r") as myfile:
	data = myfile.read()

print('')
print('')
print('############################################')
print('#   Entity Extraction                      #')
print('############################################')
print('')
print('')

response = alchemyapi.entities('text', data, {'sentiment': 1})

if response['status'] == 'OK':
    print('## Response Object ##')
    print(json.dumps(response, indent=4))
    
    print('')
    print('## Entities ##')
    for entity in response['entities']:
        print('text: ', entity['text'].encode('utf-8'))
        print('type: ', entity['type'])
        print('relevance: ', entity['relevance'])
        print('sentiment: ', entity['sentiment']['type'])
        if 'score' in entity['sentiment']:
            print('sentiment score: ' + entity['sentiment']['score'])
        print('')
else:
    print('Error in entity extraction call: ', response['statusInfo'])


print('')
print('')
print('')
print('############################################')
print('#   Keyword Extraction                     #')
print('############################################')
print('')
print('')

response = alchemyapi.keywords('text', data,  {'sentiment': 1})

print('## Keywords ##')
if response['status'] == 'OK':
    print('## Response Object ##')
    print(json.dumps(response, indent=4))
    
    print('')
    print('## Keywords ##')
    for keyword in response['keywords']:
        print('text: ', keyword['text'].encode('utf-8'))
        print('relevance: ', keyword['relevance'])
        print('sentiment: ', keyword['sentiment']['type'])
        if 'score' in keyword['sentiment']:
            print('sentiment score: ' + keyword['sentiment']['score'])
        print('')
else:
    print('Error in entity extraction call: ', response['statusInfo'])


print('')
print('')
print('')
print('############################################')
print('#   Concept Tagging                        #')
print('############################################')
print('')
print('')

response = alchemyapi.concepts('text', data)

if response['status'] == 'OK':
    print('## Object ##')
    print(json.dumps(response, indent=4))
    
    print('')
    print('## Concepts ##')
    for concept in response['concepts']:
        print('text: ', concept['text'])
        print('relevance: ', concept['relevance'])
        print('')
else:
    print('Error in concept tagging call: ', response['statusInfo'])


print('')
print('')
print('')
print('############################################')
print('#   Language Detection                     #')
print('############################################')
print('')
print('')

response = alchemyapi.language('text', data)

if response['status'] == 'OK':
    print('## Response Object ##')
    print(json.dumps(response, indent=4))

    print('')
    print('## Language ##')
    print('language: ', response['language'])
    print('iso-639-1: ', response['iso-639-1'])
    print('native speakers: ', response['native-speakers'])
    print('')
else:
    print('Error in language detection call: ', response['statusInfo'])
