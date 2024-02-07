package transactionsx

type jsonSerializedMutation struct {
	Bucket     string `json:"bkt"`
	Scope      string `json:"scp"`
	Collection string `json:"coll"`
	ID         string `json:"id"`
	Cas        string `json:"cas"`
	Type       string `json:"type"`
}

type jsonSerializedAttempt struct {
	ID struct {
		Transaction string `json:"txn"`
		Attempt     string `json:"atmpt"`
	} `json:"id"`
	ATR struct {
		Bucket     string `json:"bkt"`
		Scope      string `json:"scp"`
		Collection string `json:"coll"`
		ID         string `json:"id"`
	} `json:"atr"`
	Config struct {
		KeyValueTimeoutMs int    `json:"kvTimeoutMs"`
		DurabilityLevel   string `json:"durabilityLevel"`
		NumAtrs           int    `json:"numAtrs"`
	} `json:"config"`
	State struct {
		TimeLeftMs int `json:"timeLeftMs"`
	} `json:"state"`
	Mutations []jsonSerializedMutation `json:"mutations"`
}
