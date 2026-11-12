package elasticsearch

func Asc(field string) SortOption {
	return SortOption{Field: field, Desc: false}
}

func Desc(field string) SortOption {
	return SortOption{Field: field, Desc: true}
}
