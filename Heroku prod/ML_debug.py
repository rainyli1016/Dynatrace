import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Embedding, LSTM
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras import backend as K


class CodeDebugChatbot:
    def __init__(self, training_data):
        self.training_data = training_data
        self.tokenizer = Tokenizer()
        self.model = None
        self.max_sequence_len = None

    def preprocess_data(self):
        self.tokenizer.fit_on_texts(self.training_data)
        total_words = len(self.tokenizer.word_index) + 1
        input_sequences = []
        for line in self.training_data:
            token_list = self.tokenizer.texts_to_sequences([line])[0]
            for i in range(1, len(token_list)):
                n_gram_sequence = token_list[:i + 1]
                input_sequences.append(n_gram_sequence)
        max_sequence_len = max([len(x) for x in input_sequences])
        input_sequences = np.array(pad_sequences(
            input_sequences, maxlen=max_sequence_len, padding='pre'))
        predictors, labels = input_sequences[:, :-1], input_sequences[:, -1]
        labels = tf.keras.utils.to_categorical(labels, num_classes=total_words)
        self.max_sequence_len = max_sequence_len
        return predictors, labels, total_words

    def build_model(self, total_words):
        model = Sequential()
        model.add(Embedding(total_words, 100,
                  input_length=self.max_sequence_len - 1))
        model.add(LSTM(150))
        model.add(Dense(total_words, activation='softmax'))
        model.compile(loss='categorical_crossentropy',
                      optimizer=Adam(lr=0.01), metrics=['accuracy'])
        self.model = model

    def train_model(self, predictors, labels):
        self.model.fit(predictors, labels, epochs=100, verbose=1)

    def generate_response(self, input_text, temperature=0.5):
        input_text = input_text.lower()
        encoded_input = self.tokenizer.texts_to_sequences([input_text])[0]
        encoded_input = pad_sequences(
            [encoded_input], maxlen=self.max_sequence_len - 1, padding='pre')
        probabilities = self.model.predict(encoded_input, verbose=0)[0]
        predicted_index = self._sample_with_temperature(
            probabilities, temperature)
        output_word = ''
        for word, index in self.tokenizer.word_index.items():
            if index == predicted_index:
                output_word = word
                break
        return output_word

    @staticmethod
    def _sample_with_temperature(predictions, temperature):
        predictions = np.asarray(predictions).astype('float64')
        predictions = np.log(predictions) / temperature
        exp_predictions = np.exp(predictions)
        predictions = exp_predictions / np.sum(exp_predictions)
        probabilities = np.random.multinomial(1, predictions, 1)
        return np.argmax(probabilities)

    def save_model(self, model_path):
        self.model.save(model_path)

    @staticmethod
    def load_model(model_path):
        return tf.keras.models.load_model(model_path)

# Training data
training_data = [
    "SyntaxError: invalid syntax",
    "TypeError: unsupported operand type(s)",
    "NameError: name 'variable' is not defined",
    "IndentationError: unexpected indent",
    "KeyError: 'key'",
    "ValueError: invalid literal for int()",
]

# Step 1: Instantiate the code debugging chatbot
chatbot = CodeDebugChatbot(training_data)

# Step 2: Preprocess the data
predictors, labels, total_words = chatbot.preprocess_data()

# Step 3: Build the model
chatbot.build_model(total_words)

# Step 4: Train the model
chatbot.train_model(predictors, labels)

# Step 5: Generate a response based on input text
input_text = "Error: unsupported operand type(s)"
response = chatbot.generate_response(input_text)
print(response)  # Output: "TypeError"

# Step 6: Save the model for future use
chatbot.save_model("code_debug_model.h5")

# Step 7: Load the model
loaded_chatbot = CodeDebugChatbot.load_model("code_debug_model.h5")

# Generate a response using the loaded model
input_text = "Error: name 'variable' is not defined"
response = loaded_chatbot.generate_response(input_text)
print(response)  
