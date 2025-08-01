package cbqueryx

import (
	"errors"
	"regexp"
	"strings"
)

func parseError(errJson *queryErrorJson) *ServerError {
	var err error

	errCode := errJson.Code
	errCodeGroup := errCode / 1000

	if errCodeGroup == 4 {
		err = ErrPlanningFailure
	}
	if errCodeGroup == 5 {
		err = ErrInternalServerError
		lowerMsg := strings.ToLower(errJson.Msg)
		if strings.Contains(lowerMsg, "not enough") &&
			strings.Contains(lowerMsg, "replica") {
			err = ServerInvalidArgError{
				Argument: "NumReplicas",
				Reason:   "not enough indexer nodes to create index with replica count",
			}
		}
		if strings.Contains(lowerMsg, "build already in progress") {
			err = ErrBuildAlreadyInProgress
		}
		if strings.Contains(lowerMsg, "build index fails") && strings.Contains(lowerMsg, "index will be retried building") {
			err = ErrBuildFails
		}
		if match, matchErr := regexp.MatchString(".*?ndex .*? already exist.*", lowerMsg); matchErr == nil && match {
			err = ErrIndexExists
		}
	}
	if errCodeGroup == 12 || errCodeGroup == 14 {
		err = ErrIndexFailure
	}
	if errCodeGroup == 10 {
		err = ErrAuthenticationFailure
	}

	if errCode == 1000 {
		err = ErrWriteInReadOnlyQuery
	}
	if errCode == 1080 {
		err = ErrTimeout
	}
	if errCode == 3000 {
		err = ErrParsingFailure
	}
	if errCode == 4040 || errCode == 4050 || errCode == 4060 || errCode == 4070 || errCode == 4080 || errCode == 4090 {
		err = ErrPreparedStatementFailure
	}
	if errCode == 4300 {
		err = createResourceError(errJson.Msg, ErrIndexExists)
	}
	if errCode == 12003 {
		err = createResourceError(errJson.Msg, ErrCollectionNotFound)
	}
	if errCode == 12004 {
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
	}
	if errCode == 12009 {
		err = ErrDmlFailure

		if len(errJson.Reason) > 0 {
			if code, ok := errJson.Reason["code"]; ok {
				code = int(code.(float64))
				switch code {
				case 12033:
					err = ErrCasMismatch
				case 17014:
					err = ErrDocumentNotFound
				case 17012:
					err = ErrDocumentExists
				}
			}
		}

		if strings.Contains(strings.ToLower(errJson.Msg), "cas mismatch") {
			err = ErrCasMismatch
		}
	}
	if errCode == 12016 {
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
	}

	if errCode == 12021 {
		err = createResourceError(errJson.Msg, ErrScopeNotFound)
	}

	if errCode == 13014 {
		err = createResourceError(errJson.Msg, ErrAuthenticationFailure)
	}

	if err == nil {
		err = errors.New("unexpected query error")
	}

	return &ServerError{
		InnerError: err,
		Code:       errJson.Code,
		Msg:        errJson.Msg,
	}
}

func createResourceError(msg string, cause error) *ResourceError {
	err := &ResourceError{
		Cause: cause,
	}

	if errors.Is(err, ErrScopeNotFound) || errors.Is(err, ErrCollectionNotFound) {
		parseResourceNotFoundMsg(err, msg)
	}

	if errors.Is(err, ErrAuthenticationFailure) {
		parseAuthFailureMsg(err, msg)
	}

	if errors.Is(err, ErrIndexNotFound) || errors.Is(err, ErrIndexExists) {
		parseIndexNotFoundOrExistsMsg(err, msg)
	}

	return err
}

func parseIndexNotFoundOrExistsMsg(err *ResourceError, msg string) {
	fields := strings.Fields(msg)
	// msg for not found is of the form - "Index Not Found - cause: GSI index testingIndex not found."
	// msg for index exists is of the form - "The index NewIndex already exists."
	for i, f := range fields {
		if f == "index" {
			err.IndexName = fields[i+1]
			return
		}
	}
}

func parseResourceNotFoundMsg(err *ResourceError, msg string) {
	var path string
	fields := strings.Fields(msg)
	for _, f := range fields {
		// Resource path is of the form bucket:bucket.scope.collection
		if strings.Contains(f, ".") && strings.Contains(f, ":") {
			path = f
			break
		}
	}

	_, trimmedPath, found := strings.Cut(path, ":")
	if !found {
		return
	}
	fields = strings.Split(trimmedPath, ".")
	if errors.Is(err, ErrScopeNotFound) {
		// Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
		err.BucketName = strings.Join(fields[:len(fields)-1], ".")
		err.ScopeName = fields[len(fields)-1]
		return
	}

	if errors.Is(err, ErrCollectionNotFound) {
		err.BucketName = strings.Join(fields[:len(fields)-2], ".")
		err.ScopeName = fields[len(fields)-2]
		err.CollectionName = fields[len(fields)-1]
	}
}

func parseAuthFailureMsg(err *ResourceError, msg string) {
	var path string
	fields := strings.Fields(msg)
	for _, f := range fields {
		if strings.Contains(f, ":") {
			path = f
			break
		}
	}

	_, trimmedPath, found := strings.Cut(path, ":")
	if !found {
		return
	}

	var scopeAndCol string
	// If the bucket name contains "." then the path needs to be parsed differently. We differentiate between dots in
	// the bucket name and those separating bucket, scope and collection by the bucket name being wrapped in "`" if
	// it contains "."
	if strings.Contains(trimmedPath, "`") {
		// trimmedPath will have the form "`bucket.name`" or "`bucket.name`.scope.collection" so the fist element of fields
		// will be the empty string
		fields := strings.Split(trimmedPath, "`")
		err.BucketName = fields[1]
		if fields[2] == "" {
			return
		}
		// scopeAndCol is of the form ".scope.collection" meaning fields[1] is empty and the names are in fields[1] and
		// fields[2]
		scopeAndCol = fields[2]
		fields = strings.Split(scopeAndCol, ".")
		if len(fields) < 3 {
			return
		}
		err.ScopeName = fields[1]
		err.CollectionName = fields[2]
	} else {
		fields = strings.Split(trimmedPath, ".")
		err.BucketName = fields[0]
		if len(fields) < 3 {
			return
		}
		err.ScopeName = fields[1]
		err.CollectionName = fields[2]
	}
}
