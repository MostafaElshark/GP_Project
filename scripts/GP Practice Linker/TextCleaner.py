import re
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import defaultdict
from nltk.corpus import wordnet as wn

class TextCleaner:
    def __init__(self):
        self.tag_map = defaultdict(lambda: wn.NOUN)
        self.tag_map['J'] = wn.ADJ
        self.tag_map['V'] = wn.VERB
        self.tag_map['R'] = wn.ADV
        self.stopwords = stopwords.words('english')

    def clean_abstract(self, text):
        text = str(text)
        text = text.lower().strip()
        text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        text = self.regex_cleaning(text)
        words_list = word_tokenize(text)
        text = self.lemmatize_words(words_list)
        return ' '.join(text)

    def regex_cleaning(self, text):
        replacements = {
    r'\bhosp\b': 'hospital',
    r'\bhospitals\b': 'hospital',
    r'\bhosps\b': 'hospital',
    r'\buni\b': 'university',
    r'\buniv\b': 'university',
    r'\bunis\b': 'university',
    r'\buniversities\b': 'university',
    r'\buniversitys\b': 'university',
    r'\bassoc\b': 'association',
    r'\bsoc\b': 'society',
    r'\bft\b': 'foundation trust',
    r'\btrst\b': 'trust',
    r'\btrs\b': 'trust',
    r'\&\b': 'and',
    r'\bmed\b': 'medical',
    r'\bdr\b': 'doctor',
    r'\bdrs\b': 'doctors',
    r'\bdept\b': 'department',
    r'\bctr\b': 'centre',
    r'\brheumatol\b': 'rheumatology',
    r'\bassoc\b': 'association',
    r'\bhlth\b': 'health',
    r'\bltd\b': 'limited',
    r'\bpcn\b': 'primary care network',
    r'\blmc\b': 'local medical committee',
    r'\brcgp\b': 'royal college of general practitioners'
    }

        for pattern, replacement in replacements.items():
            text = re.sub(pattern, replacement, text)
        text = text.split('-')[0].strip()
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def lemmatize_words(self, words_list):
        final_words = []
        word_lemmatized = WordNetLemmatizer()
        for word, tag in pos_tag(words_list):
            if word not in self.stopwords and word.isalpha() and len(word) > 2:
                word_final = word_lemmatized.lemmatize(word, self.tag_map[tag[0]])
                final_words.append(word_final)
        return final_words
