package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/DeedleFake/ventral/blockstore"
	"github.com/restic/chunker"
)

func read(s blockstore.Store, args []string) {
	for _, block := range args {
		func() {
			r, err := s.Read(block)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to open %q: %v\n", block, err)
				os.Exit(1)
			}
			defer r.Close()

			_, err = io.Copy(os.Stdout, r)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to read %q: %v\n", block, err)
				os.Exit(1)
			}
		}()
	}
}

func concat(args []string) io.Reader {
	if len(args) == 0 {
		args = []string{"-"}
	}

	files := make([]io.Reader, 0, len(args))
	for _, arg := range args {
		if arg == "-" {
			files = append(files, os.Stdin)
			continue
		}

		files = append(files, &lazyFile{path: arg})
	}

	return io.MultiReader(files...)
}

func write(s blockstore.Store, args []string) {
	w, id, err := s.Write()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create writer: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	files := concat(args)

	_, err = io.Copy(w, files)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to write data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%x\n", id)
}

func chunk(s blockstore.Store, args []string) {
	const pol = 0x3ffa0afcf682c7

	files := concat(args)
	c := chunker.New(files, pol)

	var ids [][]byte
	var buf []byte
	for {
		n, err := c.Next(buf[:0])
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Fprintf(os.Stderr, "Error: failed to create chunk: %v\n", err)
			os.Exit(1)
		}

		func() {
			w, id, err := s.Write()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to create writer: %v\n", err)
				os.Exit(1)
			}
			defer w.Close()

			_, err = w.Write(n.Data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to write chunk: %v\n", err)
				os.Exit(1)
			}

			ids = append(ids, id)
			buf = n.Data
		}()
	}

	for _, id := range ids {
		fmt.Printf("%x\n", id)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %v [options] <command> <args>

Options:
	-r <path>
		root of the blockstore (default $BLOCKSTORE_ROOT)

Commands:
	read <blocks...>
		read blocks from the blockstore to stdout

	write [files...]
		write the data in files, or stdin if none are specified, to the
		blockstore as a single, concatenated block, then prints the ID of
		the block written

	chunk [files...]
		chunk writes the data in files, or stdin if none are specified, to
		the blockstore in content-defined chunks, then prints the IDs of
		the blocks written, in the order that they were written
`, filepath.Base(os.Args[0]))
	}
	root := flag.String("r", os.Getenv("BLOCKSTORE_ROOT"), "root of the blockstore")
	flag.Parse()

	if *root == "" {
		fmt.Fprintln(os.Stderr, "Error: no root specified")
		os.Exit(2)
	}

	mode := flag.Arg(0)
	if mode == "" {
		fmt.Fprintln(os.Stderr, "Error: no command specified")
		os.Exit(2)
	}

	cmd := map[string]func(blockstore.Store, []string){
		"read":  read,
		"write": write,
		"chunk": chunk,
	}[mode]
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "Error: unknown command: %q\n", mode)
		os.Exit(2)
	}

	s, err := blockstore.Dir(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to open the store: %v\n", err)
		os.Exit(1)
	}

	cmd(s, flag.Args()[1:])
}
