import csv
import nltk
import pickle
import re
from spellchecker import SpellChecker

import os
abbrev_file = 'word_dicts\\abbrev_pkl.txt'
tech_file = 'word_dicts\\tech_terms_pkl.txt'
this_file = os.path.abspath(__file__)
this_dir = os.path.dirname(this_file)
abbrev_path = os.path.join(this_dir, abbrev_file)
tech_path = os.path.join(this_dir, tech_file)

# Load the abbreviations dictionary to be used in correct_spelling
with open(abbrev_path, 'rb') as fh:
    AB_DICT = pickle.load(fh)

# Load new words, that shouldn't be changed when spellchecking, to be inserted into SpellChecker 
#   NEW_WORDS includes tech abbreviations
with open(tech_path, 'rb') as fh:
    NEW_WORDS = pickle.load(fh)


def str_impute_lower(df, cols):
    """
    Turning columns containing strings into all lowercase and imputing empty string where Null
    :param df: pandas data frame
    :param cols: list of names of columns that are strings
    :return: pandas data frame
    """
    df.fillna({cols[i]: '' for i in range(len(cols))}, inplace=True)
    for column in cols:
        df[column] = df[column].str.lower()
    return df


def write_list_csv(file, str_list):
    """
    Write a list object to a csv
    :param file: name of the path/file to write to
    :param str_list: list of strings
    :return: none
    """
    with open(file, 'w', newline='') as res_fy:
        wr = csv.writer(res_fy)
        for s in str_list:
            wr.writerow([s])


def tokenize_col(docs):
    """
    split a list of strings into a list of lists of tokens
        ["this is", "the example"] to [["this", "is"], ["the", "example"]]
    :param docs: list of strings to be tokenized
    :return: list of list of tokens
    """
    token_docs = []
    for doc in docs:
        # list of set to get unique words only
        words = list(set(nltk.word_tokenize(doc)))
        token_docs.append(words)
    return token_docs


def remove_punctuation(words):
    """
    removes punctuation from a list of strings (usually tokens from tokenize_col)
    :param words: list of strings/tokens to clean
    :return: list of strings
    """
    new_words = []
    for word in words:
        clean_words = re.split(r'\W', word)
        non_empty = [w for w in clean_words if w != ""]
        new_words.extend(non_empty)
    return new_words


def separate_level(words):
    """
    separating digits from levels in list of strings (usually tokens from tokenize_col)
        ['l2', 'level3'] becomes ['l','2', 'level','3']
    :param words: list of strings/tokens
    :return: list of strings
    """
    # sub l2, level2, for digit
    new_words = []
    for word in words:
        digit_search = re.search(r'(\w*l)(\d+\.*\d*)', word)  
        if digit_search:
            name = digit_search.group(1)
            number = digit_search.group(2)
            new_words.extend([name, number])
        else:
            new_words.append(word)
    return new_words


##############################################
# Find Tech Terms
#     misspelled words occurring more than once
#     small misspelled words for abbrevs
##############################################


# Set one instance of spellchecker.SpellChecker
SC = SpellChecker()
# Add NEW_WORDS to list of correct words in SpellChecker
#   NEW_WORDS are created from tech terms in the document
SC.word_frequency.load_words(NEW_WORDS)


def find_misspelled(words):
    """
    Using spellchecker package to find misspelled words (usually tokens from tokenize_col)
    :param words: list of strings/tokens
    :return: list of misspelled words if any
    """
    mis = SC.unknown(words)
    return list(mis) if mis else list()


def word_occur_more(source_list, tok_list):
    """
    See if the tokens in tok_list appear in source_list more than once
    :param source_list: original string col from df (eg laboredge.position_title)
    :param tok_list: list of tokens (usually abbreviations and misspelled words)
    :return: list of tokens
    """
    freq_tok = []
    for tok in tok_list:
        pattern = re.compile(r'(^|\W)' + tok + r'($|\W)')
        ind_match = source_list.str.contains(pattern, regex=True)
        # if the abbrev occurs more than once AND doesn't start with a digit (3rd)
        if sum(ind_match) > 1 and re.match(r'\D', tok):
            freq_tok.append(tok)
    return freq_tok


def correct_spelling(words, abbrev_dict = AB_DICT):
    """
    Corrects spelling in a list of strings
        if word is misspelled, give corrected word
        elif word is an abbreviation, give full word
        else keep word as is
    :param words: list of strings to evaluate
    :param abbrev_dict: dictionary with abbreviation as key and full word as value
    :return: list of corrected words
    """
    new_words = []
    for word in words:
        if SC.unknown([word]):
            new = SC.correction(word)
            new_words.append(new)
        elif word in abbrev_dict.keys():
            new = abbrev_dict[word]
            new_words.append(new)
        else:
            new_words.append(word)
    return new_words


LEMMER = nltk.stem.WordNetLemmatizer()
def lemma_tokens(words, lemmer=LEMMER):
    """
    lemmatize list of strings(usually tokens from tokenize_col)
    :param words: list of strings
    :param lemmer: instance of nltk WordNetLemmatizer
    :return: lemmatized words
    """
    new_words = []
    for word in words:
        lemma = lemmer.lemmatize(word)
        new_words.append(lemma)
    return new_words


def normalize_tokens(docs):
    """
    clean a list of lists of tokens(usually tokens from tokenize_col)
    :param docs: list of lists of tokens
    :return: list of lists of tokens
    """
    clean_docs = []
    for doc in docs:
        doc = remove_punctuation(doc)
        doc = separate_level(doc)
        doc = correct_spelling(doc)
        doc = lemma_tokens(doc)
        clean_docs.append(doc)
    return clean_docs
