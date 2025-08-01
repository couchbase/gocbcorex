package cbqueryx

import (
	"errors"
	"regexp"
	"strings"
)

var indexExistsRegex = regexp.MustCompile(`(?i)index .*? already exist`)

func parseError(errJson *queryErrorJson) *ServerError {
	var err error

	errCode := errJson.Code
	errCodeGroup := errCode / 1000

	switch errCodeGroup {
	case 4:
		err = ErrPlanningFailure
	case 5:
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
		if indexExistsRegex.MatchString(lowerMsg) {
			err = ErrIndexExists
		}
	case 12, 14:
		err = ErrIndexFailure
	case 10:
		err = ErrAuthenticationFailure
	}

	switch errCode {
	case 1000:
		err = ErrWriteInReadOnlyQuery
	case 1080:
		err = ErrTimeout
	case 2120:
		err = ErrAuthenticationFailure
	case 3000:
		err = ErrParsingFailure
	case 4040, 4050, 4060, 4070, 4080, 4090:
		err = ErrPreparedStatementFailure
	case 4300:
		err = createResourceError(errJson.Msg, ErrIndexExists)
	case 12003:
		lowerMsg := strings.ToLower(errJson.Msg)
		if strings.Contains(lowerMsg, "bucket") {
			err = createResourceError(errJson.Msg, ErrBucketNotFound)
		} else {
			err = createResourceError(errJson.Msg, ErrCollectionNotFound)
		}
	case 12004:
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
	case 12009:
		err = ErrDmlFailure

		if errJson.Reason != nil {
			switch errJson.Reason.Code {
			case 12033:
				err = ErrCasMismatch
			case 17014:
				err = ErrDocumentNotFound
			case 17012:
				err = ErrDocumentExists
			}
		}

		if strings.Contains(strings.ToLower(errJson.Msg), "cas mismatch") {
			err = ErrCasMismatch
		}
	case 12016:
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
	case 12021:
		err = createResourceError(errJson.Msg, ErrScopeNotFound)
	case 13014:
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

	if errors.Is(err, ErrKeyspaceNotFound) ||
		errors.Is(err, ErrBucketNotFound) ||
		errors.Is(err, ErrScopeNotFound) ||
		errors.Is(err, ErrCollectionNotFound) {
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
		// Resource path is of the forms:
		//   namespace:bucket
		//   namespace:bucket.name.with.dots
		//   namespace:bucket.scope.collection

		colonIdx := strings.Index(f, ":")
		if colonIdx == -1 || colonIdx == 0 || colonIdx == len(f)-1 {
			continue
		}

		path = f
		break
	}

	_, trimmedPath, found := strings.Cut(path, ":")
	if !found {
		return
	}

	fields = strings.Split(trimmedPath, ".")

	if errors.Is(err, ErrBucketNotFound) {
		err.BucketName = strings.Join(fields, ".")
	}

	if errors.Is(err, ErrScopeNotFound) {
		// Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
		err.BucketName = strings.Join(fields[:len(fields)-1], ".")
		err.ScopeName = fields[len(fields)-1]
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
