package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/DeedleFake/ventral/blockstore"
)

func read(fs blockstore.FileSystem, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: no block IDs given")
		os.Exit(2)
	}

	r, err := blockstore.Read(fs, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to open blockstore: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

	_, err = io.Copy(os.Stdout, r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read blocks: %v\n", err)
		os.Exit(1)
	}
}

func write(fs blockstore.FileSystem, args []string) {
	w := blockstore.Write(fs, blockstore.NewRabinChunker((1<<13)-1, 101))
	defer func() {
		err := w.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to write data: %v\n", err)
			os.Exit(1)
		}

		for _, block := range w.Blocks() {
			fmt.Println(block)
		}
	}()

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

	_, err := io.Copy(w, io.MultiReader(files...))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to write data: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %v [options] <command> <args>

Commands:
	read <blocks...>
		read blocks from the blockstore to stdout

	write [files...]
		write the data in files, or stdin if none are specified, to the
		blockstore, then prints the IDs of the blocks written

Options:
	-r <path>
		root of the blockstore (default $BLOCKSTORE_ROOT)

	-gzip
		use gzip compression
`, filepath.Base(os.Args[0]))
	}
	root := flag.String("r", os.Getenv("BLOCKSTORE_ROOT"), "root of the blockstore")
	gzip := flag.Bool("gzip", false, "use gzip compression")
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

	cmd := map[string]func(blockstore.FileSystem, []string){
		"read":  read,
		"write": write,
	}[mode]
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "Error: unknown command: %q\n", mode)
		os.Exit(2)
	}

	fs := blockstore.Dir(*root)
	if *gzip {
		fs = blockstore.Gzip(fs)
	}

	cmd(fs, flag.Args()[1:])
}
