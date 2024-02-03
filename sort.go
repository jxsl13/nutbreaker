package nutbreaker

type byIP []boundary

func (a byIP) Len() int      { return len(a) }
func (a byIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byIP) Less(i, j int) bool {
	if a[i].Score == negInf {
		return true
	} else if a[j].Score == posInf {
		return false
	}
	return a[i].IP.Compare(a[j].IP) < 0
}
