package raft

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
type Log struct {
	Entries []Entry
}

func (l *Log) getLastEntryL() *Entry {
	return &l.Entries[len(l.Entries)-1]
}

func (l *Log) getLastTermAndIndexL() (int, int) {
	entry := l.getLastEntryL()
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Term, entry.Index
}
func (l *Log) getLastTermL() int {
	entry := l.getLastEntryL()
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Term
}
func (l *Log) getLastIndexL() int {
	entry := l.getLastEntryL()
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Index
}

func (l *Log) getIndexTermAndIndexL(index int) (int, int) {
	entry := l.Entries[index]
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Term, entry.Index
}
func (l *Log) getIndexTermL(index int) int {
	entry := l.Entries[index]
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Term
}
func (l *Log) getIndexIndexL(index int) int {
	entry := l.Entries[index]
	//fmt.Printf("%v %v %v\n", len(l.Entries), entry.Term, entry.Index)
	return entry.Index
}
func (l *Log) getLen() int {
	return len(l.Entries)
}

func (l *Log) appendEntryL(entry Entry) {
	l.Entries = append(l.Entries, entry)
}
