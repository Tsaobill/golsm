package skiplist

// 整数比较器
type IntComparator struct{}

func (cmp IntComparator) Compare(a, b interface{}) int {
	aInt, aOk := a.(int)
	bInt, bOk := b.(int)

	if !aOk || !bOk {
		panic("IntComparator: invalid type")
	}

	if aInt < bInt {
		return -1
	} else if aInt > bInt {
		return 1
	}
	return 0
}

// 字符串比较器
type StringComparator struct{}

func (cmp StringComparator) Compare(a, b interface{}) int {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)

	if !aOk || !bOk {
		panic("StringComparator: invalid type")
	}

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// 字节数组比较器(常用于LSM树中的键比较)
type BytesComparator struct{}

func (cmp BytesComparator) Compare(a, b interface{}) int {
	aBytes, aOk := a.([]byte)
	bBytes, bOk := b.([]byte)

	if !aOk || !bOk {
		panic("BytesComparator: invalid type")
	}

	aLen, bLen := len(aBytes), len(bBytes)
	minLen := aLen
	if bLen < minLen {
		minLen = bLen
	}

	for i := 0; i < minLen; i++ {
		if aBytes[i] < bBytes[i] {
			return -1
		} else if aBytes[i] > bBytes[i] {
			return 1
		}
	}

	if aLen < bLen {
		return -1
	} else if aLen > bLen {
		return 1
	}
	return 0
}
