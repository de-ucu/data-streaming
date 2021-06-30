import fasttext


model = fasttext.load_model('/model/model.bin')

def preprocess(text):
    new_text = []

    for t in text.split(' '):
        t = '' if t.startswith('@') or t.startswith('http') or t.startswith('#') else t
        new_text.append(t)

    return ' '.join(new_text).replace('\n', 'new').replace(r'\s+', ' ').strip()

def get_language(messages):
    texts = [preprocess(message) for message in messages]
    predictions = model.predict(texts)
    return [p[0][-2:] for p in predictions[0]]
