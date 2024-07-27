import pandas as pd
import os
from Constants import *
import random
import Helper
import re

df_is_in_search_result = pd.read_csv(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.IS_TWITTER_IN_SEARCH_RESULT.value + Extension.CSV.value))


#list of ein for which their twitter page is in our search result
ein_list = df_is_in_search_result[df_is_in_search_result["is_in_search_result"] == 1].ein.tolist()

except_eins = set([61738472,
203752439,
203752439,
205558440,
237083305,
237164136,
237178593,
237221778,
237279442,
237339565,
270946211,
274314310,
275005531,
320405596,
352308704,
453232829,
454875183,
471970335,
472311473,
472490986,
474172696,
510174165,
521339764,
581392796,
720376063,
720394077,
720457699,
720475545])

print(len(ein_list))
ein_list = [ein for ein in ein_list if ein not in except_eins]
print(len(ein_list))

random.Random(Constants.RANDOM_STATE.value).shuffle(ein_list)

train_size = int(len(ein_list) * 0.7) #70% of the orgs search result will be used for training
test_size = len(ein_list) - train_size

train_eins = ein_list[:train_size]
test_eins = ein_list[train_size:]

df_combined_similarity_score = pd.read_csv(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.COMBINED_SIMILARITY_SCORE.value + Extension.CSV.value))

df = pd.merge(df_combined_similarity_score, df_is_in_search_result[["ein", "twitter_id", "is_in_search_result"]], how = 'left', on = 'ein')
df.rename(columns = {'twitter_id_x':'twitter_id', 'twitter_id_y':'twitter_id_orig'}, inplace = True)
df = df[df["is_in_search_result"] == 1]

#location and description contains a lot of nan value. replace it with empty string
df["name"] = df["name"].fillna('')
df["location"] = df["location"].fillna('')
df["description"] = df["description"].fillna('')


#most of location and description string are bytestring. convert them
df["location"] = df["location"].apply(Helper.decodeByteString)
df["description"] = df["description"].apply(Helper.decodeByteString)

#lower case twitter name, location, description
df["name"] = df["name"].str.lower()
df["location"] = df["location"].str.lower()
df["description"] = df["description"].str.lower()


def label_is_same_city(row, city_names, zip_codes):
    if any(city_name in row["location"] for city_name in city_names):
        return 1
    if any(city_name in row["name"] for city_name in city_names):
        return 1
    if any(city_name in row["description"] for city_name in city_names):
        return 1
    zip_code = Helper.get_trailing_digits(row["location"])
    if zip_code:
        if int(zip_code) in zip_codes:
            return 1
    return 0

df["is_same_city"] = df.apply(lambda row : label_is_same_city(row, city_names = ["baton rouge", "batonrouge"], zip_codes=BTR_ZIP_CODES),
                                                              axis = 1)

def label_is_same_state(row, state_name, state_abbv):
    if row["is_same_city"]:
        return 1
    if state_name in row["location"]:
        return 1
    if state_name in row["name"]:
        return 1
    if state_name in row["description"]:
        return 1  
    # if the address field contains state abbreviation. (this might create problem when a place name matches with state abbv. 
    # For example la => los angeles; la => ouisiana
    words = [word.strip() for word in re.split('[^a-zA-Z]', row["location"]) if word]
    if state_abbv in words:
        return 1
    return 0

df["is_same_state"] = df.apply(lambda row : label_is_same_state(row, state_name = "louisiana", state_abbv = "la"),
                                                                axis = 1)

df["class"] = (df["twitter_id"] == df["twitter_id_orig"]).astype(int)

df_train = df[df["ein"].isin(train_eins)]
df_test = df[df["ein"].isin(test_eins)]
similary_scores = ["jaro_similarity", "jaro_winkler_similarity", "fuzz_ratio", "fuzz_partial_ratio", "fuzz_token_sort_ratio", "fuzz_token_set_ratio"]
other_features = ["is_same_city", "is_same_state"]

features = similary_scores + other_features
"""
#we will remove some candidates from train data which have high similarity score but not actual page

delete_indices = set()
for ein in train_eins:
    candidate_count = len(df_train[df_train["ein"] == ein])
    for similary_score in similary_scores:
        delete_indices.update(df_train[df_train["ein"] == ein].sort_values(by = [similary_score], ascending = [False]).head(int(candidate_count * 0.2)).index)

positive_train_indices = set(df_train[df_train["class"] == 1 & df_train["ein"].isin(train_eins)].index)
delete_indices = delete_indices - positive_train_indices

df_train.drop(index = delete_indices, inplace=True)
"""
X_train = df_train.drop('class', axis = 1)
X_test = df_test.drop('class', axis = 1)

X_train_data = X_train[features]
X_test_data = X_test[features]

y_train = df_train["class"]
y_test = df_test["class"]

# from sklearn import svm
# clf_svm = svm.SVC()
# clf_svm.fit(X_train_data, y_train)
# y_pred = clf_svm.predict(X_test_data)
# df_train

# from sklearn.tree import DecisionTreeClassifier
# clf_dt = DecisionTreeClassifier()
# clf_dt.fit(X_train_data, y_train)
# clf_dt.predict_proba(X_test_data)

from sklearn.ensemble import RandomForestClassifier
clf_rf = RandomForestClassifier(class_weight='balanced', random_state=Constants.RANDOM_STATE.value)
clf_rf.fit(X_train_data, y_train)
probabilities = clf_rf.predict_proba(X_test_data)
# Add probability columns to the DataFrame
df_test['prob_class_0'] = probabilities[:, 0]
df_test['prob_class_1'] = probabilities[:, 1]
df_test.to_csv(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.CLASSIFIED_DATA.value + Extension.CSV.value), index = False, encoding = 'utf-8')














