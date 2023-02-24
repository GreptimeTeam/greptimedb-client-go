package model

import "errors"

var (
	NilPointerErr error = errors.New("nil")
	TypeNotMatchErr error = errors.New("the dataType should be consistent")
)