from html import unescape
from transformers import pipeline

import pandas as pd
import torch
import re

from langdetect import detect, LangDetectException
from tqdm import tqdm
from constants import CURR_PATH, CURR_YTBR


def clean_comments_data(df):

    # drop unused columns
    df = df.drop(columns=[
        "comment_like_count", "Unnamed: 0", 
        "comment_publish_date", "comment_reply_count", "is_reply"
    ])

    # regex pattern for emoji removal
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # various symbols
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"
        "\u3030"
        "]+", flags=re.UNICODE)

    # clean HTML, special characters, and emojis
    def clean_text(text):
        text = unescape(str(text))               # decode HTML entities
        text = re.sub(r'<[^>]+>', '', text)      # remove HTML tags
        text = emoji_pattern.sub('', text)       # remove emojis
        text = re.sub(r'&#39;', "'", text)       # replace HTML apostrophes
        return text.strip()

    df['comment_text'] = df['comment_text'].apply(clean_text)
    df['comment_text'] = df['comment_text'].str.lower()

    # handle missing values
    df = df.dropna(subset=['video_id', 'comment_id',
                   'comment_text', 'comment_author_channel_id'])

    # clean whitespace in text columns
    text_cols = ['video_title', 'comment_text']
    for col in text_cols:
        df[col] = df[col].str.strip()

    return df


def classify_comments(df):
    if not torch.cuda.is_available():
        raise RuntimeError("GPU not available. CUDA-enabled device required.")

    BATCH_SIZE = 64  # Typical values: 64-256 for modern GPUs
    MIN_TEXT_LENGTH = 3  # Skip very short texts

    language_detector = pipeline(
        "text-classification",
        model="papluca/xlm-roberta-base-language-detection",
        device=0,
        batch_size=BATCH_SIZE,
        top_k=1
    )

    # Pre-process texts
    texts = df['comment_text'].tolist()
    valid_indices = []
    valid_texts = []

    # Filter valid texts and record their positions
    for idx, text in enumerate(texts):
        if isinstance(text, str) and len(text.strip()) >= MIN_TEXT_LENGTH:
            valid_indices.append(idx)
            valid_texts.append(text.strip())

    # Batch processing with progress
    results = []
    total_batches = (len(valid_texts) + BATCH_SIZE - 1) // BATCH_SIZE

    with tqdm(total=total_batches, desc="Classifying", unit="batch") as pbar:
        for i in range(0, len(valid_texts), BATCH_SIZE):
            batch = valid_texts[i:i+BATCH_SIZE]
            try:
                batch_results = language_detector(batch)
                results.extend([(res[0]['label'], res[0]['score'])
                               for res in batch_results])
            except Exception as e:
                results.extend([('unknown', 0.0)] * len(batch))
            pbar.update(1)
            pbar.set_postfix({
                'speed': f"{pbar.format_dict['rate']:.1f} samples/s",
                'current': f"{min(i+BATCH_SIZE, len(valid_texts))}/{len(valid_texts)}"
            })

    # Reconstruct full results
    lang_list = ['unknown'] * len(df)
    prob_list = [0.0] * len(df)

    for idx, (lang, prob) in zip(valid_indices, results):
        lang_list[idx] = lang
        prob_list[idx] = prob

    df['lang'] = lang_list
    df['prob'] = prob_list

    return df


def filter_by_langs(df, text_col='comment_text'):
    target_languages = ['ar', 'en', 'fr', 'de', 'hi', 'it', 'sp', 'pt']

    def isin_target_langs(text):
        try:
            return detect(text) in target_languages
        except (LangDetectException, TypeError):
            return False

    mask = df[text_col].apply(isin_target_langs)
    return df[mask].reset_index(drop=True)

import time

def clean(df):
    print(f"processing... {CURR_YTBR}")
    cleaned_df = clean_comments_data(df)
    print("cleaned...")

    print("filtering out non english comments...")

    start = time.time()
    cleaned_df = filter_by_langs(cleaned_df)
    end = time.time()

    print(f"Filtering took {end - start:.2f} seconds.")

    print("done.", cleaned_df.info())
    cleaned_df.to_csv(f"{CURR_PATH}cleaned_comments.csv", index=False)
