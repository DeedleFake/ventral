ventral
=======

[![GoDoc](http://www.godoc.org/github.com/DeedleFake/ventral?status.svg)](http://www.godoc.org/github.com/DeedleFake/ventral)
[![Go Report Card](https://goreportcard.com/badge/github.com/DeedleFake/ventral)](https://goreportcard.com/report/github.com/DeedleFake/ventral)

*Note: This section is a lie. Implementation is currently in progress.*

ventral is a simple permanent data store for local network use inspired by [venti][venti] and [vac][vac]. A number of changes have been made from venti, however, such as replacing the custom TCP-based protocol that venti used with a simple HTTP API and implementing variable-sized chunk deduplication and compression.

[venti]: https://9fans.github.io/plan9port/man/man7/venti.html
[vac]: https://9fans.github.io/plan9port/man/man1/vac.html
