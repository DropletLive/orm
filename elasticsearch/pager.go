package elasticsearch

type Pager struct {
	Limit  int
	Offset int
}

func NewPager(page, count int) *Pager {
	if page < 1 {
		page = 1
	}

	if count < 0 {
		count = 0
	}

	return &Pager{
		Limit:  count,
		Offset: (page - 1) * count,
	}
}

func ResolvePagination(pager *Pager, fallbackLimit, maxLimit int) (int, int) {
	limit := fallbackLimit
	offset := 0

	if pager != nil {
		limit = pager.Limit
		offset = pager.Offset
	}

	limit = ClampInt(limit, fallbackLimit, maxLimit)

	if offset < 0 {
		offset = 0
	}

	return limit, offset
}

func ClampInt(value, fallback, maxValue int) int {
	switch {
	case value <= 0:
		return fallback
	case value > maxValue:
		return maxValue
	default:
		return value
	}
}
