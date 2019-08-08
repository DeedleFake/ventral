// Package blockstore provides utilities for interfacing block-based storage.
//
// ventral's block storage system stores data as collections of
// blocks. Each block is stored in a filename of its own hash, inside
// of a hash-prefix subdirectory, and a "file" is an ordered
// collection of block hashes. In other words, a "file" might look
// like
//
//    11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1
//    d56689f1e89a5029edf549153a4df0419343e0025a92a91a086d2225e26a8938
//
// and this file would be stored on disk as
//
//    <store root>
//     |-- 11/
//     |    |-- 11586d2eb43b73e539caa3d158c883336c0e2c904b309c0c5ffe2c9b83d562a1
//     |-- d5/
//          |-- d56689f1e89a5029edf549153a4df0419343e0025a92a91a086d2225e26a8938
//
// Reading this file would consist of essentially concatenating the
// contents of these two blocks.
package blockstore
