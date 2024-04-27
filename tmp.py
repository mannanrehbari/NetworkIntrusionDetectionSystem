df_kdd = pd.read_pickle("./data/KDDCUP/KDDCUP_DF_Preprocessed.pkl")
sc = MinMaxScaler()
X = sc.fit_transform(X)


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)


def create_sequences(data, seq_length):
    xs = []
    for i in range(len(data) - seq_length + 1):
        x = data[i:(i + seq_length)]
        xs.append(x)
    return np.array(xs)


seq_length = 40  # Specify your sequence length here

X_train_seq = create_sequences(X_train, seq_length)
X_test_seq = create_sequences(X_test, seq_length)

X_train_seq = X_train_seq.reshape((-1, seq_length, X_train.shape[1]))
X_test_seq = X_test_seq.reshape((-1, seq_length, X_test.shape[1]))

y_train_seq = y_train[seq_length - 1:]
y_test_seq = y_test[seq_length - 1:]


model_lstm = Sequential([
    LSTM(80, input_shape=(seq_length, X_train.shape[1])),
    Dense(1, activation='sigmoid')
])


model_lstm.compile(
    optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

history = model_lstm.fit(X_train_seq, y_train_seq,
                         epochs=10, batch_size=64, validation_split=0.2)

test_loss, test_acc = model_lstm.evaluate(X_test_seq, y_test_seq)
print(f"Test Accuracy: {test_acc:.4f}%")


model_lstm.save("./models/kdd_model_lstm.keras")


# This is in another jupyter notebook
model_lstm = load_model('./models/kdd_model_lstm.keras')


def create_sequences(data, seq_length=1):
    xs = []
    dataLen = 40
    for i in range(dataLen - seq_length + 1):
        x = data[i:(i + seq_length)]
        xs.append(x)
    return np.array(xs)


class PredictionModel:
    def __init__(self, model):
        self.model = model

    def predict_LSTM(self, data):
        dataList = data.split(',')
        dataNumeric = np.array(dataList, dtype=float)
        features = dataNumeric[:-1]
        features = features.reshape((1, 1, 40))
        prediction = self.model.predict(features)
        return prediction


class RealTimePredictor:
    def __init__(self, model, topic, brokers):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            auto_offset_reset='earliest',  # Start reading at the earliest message
            # Deserialize messages as UTF-8 encoded strings
            value_deserializer=lambda x: x.decode('utf-8')
        )
        self.model = model
        self.totalCount = 0
        self.maxCount = 98805

    def consume_messages_lstm(self):
        try:
            for message in self.consumer:
                data = message.value
                prediction = self.model.predict_LSTM(data)
                self.totalCount += 1
                print("Processed {} test items".format(self.totalCount))
                if self.totalCount == self.maxCount:
                    print('Processed all test items: {}'.format(self.totalCount))
                    break
        finally:
            self.consumer.close()


topic = "streamOut26A"
brokers = "localhost:9092"

lstm = PredictionModel(model_lstm)
kafkaConsumerLSTM = RealTimePredictor(lstm, topic, brokers)
startTime = time.time()
kafkaConsumerLSTM.consume_messages_lstm()
endTime = time.time()
print('LSTM processing time: {} seconds'.format(endTime - startTime))
