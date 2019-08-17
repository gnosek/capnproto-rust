// Copyright (c) 2017 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

extern crate capnp;

use capnp::message;

#[test]
fn canonicalize_succeeds_on_null_message() {
    let segment: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();
    assert_eq!(&canonical_words[..], segment);
}

#[test]
fn dont_truncate_struct_too_far() {
    let segment: &[u8] = &[
        // Struct pointer, body immediately follows, three data words
        0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,

        // First data word
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,

        // Second data word, all zero except most significant bit
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,

        // Third data word, all zero
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonicalized = message.canonicalize().unwrap();

    let canonical_segment: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    ];

    assert_eq!(&canonicalized[..], canonical_segment);
}


#[test]
fn dont_truncate_struct_list_too_far() {
    let segment: &[u8] = &[
        // Struct pointer, body immediately follows, one pointer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,

        // List pointer, no offset, inline composite, three words long
        0x01, 0x00, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x00,

        // Tag word, list has one element with three data words and no pointers
        0x04, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,

        // First data word
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,

        // Second data word, all zero except most significant bit
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,

        // Third data word, all zero
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonicalized = message.canonicalize().unwrap();

    let canonical_segment: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    ];

    assert_eq!(&canonicalized[..], canonical_segment);
}

#[test]
fn canonical_non_null_empty_struct_field() {
    let segment: &[u8] = &[
        // Struct pointer, body immediately follows, two pointer fields, no data.
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,

        // First pointer field, struct, offset of 1, data size 1, no pointers.
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        // Non-null pointer to empty struct.
        0xfc, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,

        // Body of struct filled with non-zero data.
        0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().unwrap());
}

#[test]
fn pointer_to_empty_struct_preorder_not_canonical() {
    let segment: &[u8] = &[
        // Struct pointer, body immediately follows, two pointer fields, no data.
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,

        // First pointer field, struct, offset of 1, data size 1, no pointers.
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        // Non-null pointer to empty struct. Offset puts it in "preorder". Would need to have
        // an offset of -1 to be canonical.
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

        // Body of struct filled with non-zero data.
        0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn is_canonical_requires_pointer_preorder() {
    let segment: &[u8] = &[
        //Struct pointer, data immediately follows, two pointer fields, no data
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,

        //Pointer field 1, pointing to the last entry, data size 1, no pointer
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        //Pointer field 2, pointing to the next entry, data size 2, no pointer
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        //Data for field 2
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,

        //Data for field 1
        0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn is_canonical_requires_dense_packing() {
    let segment: &[u8] = &[
        //Struct pointer, data after a gap
        0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        //The gap
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

        //Data for field 1
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn simple_multisegment_message() {
    let segment0: &[u8] = &[
        //Far pointer to next segment
        0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    ];

    let segment1: &[u8] = &[
        //Struct pointer (needed to make the far pointer legal)
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        //Dummy data
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ];

    let segments = &[segment0, segment1];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonicalized = message.canonicalize().unwrap();
    let canonical_segment: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ];

    assert_eq!(&canonicalized[..], canonical_segment);
}

#[test]
fn multisegment_only_first_segment_used() {
    // A segment with a canonicalized struct.
    let segment0: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ];

    let segment1: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment0, segment1];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonicalized = message.canonicalize().unwrap();
    let canonical_segment: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ];

    assert_eq!(&canonicalized[..], canonical_segment);
}

#[test]
fn is_canonical_requires_truncation_of_0_valued_struct_fields() {
    let segment: &[u8] = &[
        //Struct pointer, data immediately follows
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        //Default data value, should have been truncated
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn is_canonical_rejects_unused_trailing_words() {
    let segment: &[u8] = &[
        // Struct pointer, data in next word
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        // Data section of struct
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,

        // Trailing zero word
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn empty_inline_composite_list_of_0_sized_structs() {
    let segment: &[u8] = &[
        // Struct pointer, pointer in next word
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,

        // List pointer, inline composite, zero words long
        0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,

        // Tag word
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();
    assert_eq!(segment, &canonical_words[..]);
}

#[test]
fn inline_composite_list_with_void_list() {
    let segment: &[u8] = &[
        // List, inline composite
        0x01, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x00, 0x00,

        // One element, one pointer
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,

        // List of 1 VOID
        0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();
    assert_eq!(segment, &canonical_words[..]);
}

#[test]
fn is_canonical_rejects_inline_composite_list_with_inaccurate_word_length() {
    let segment: &[u8] = &[
        // Struct pointer, no offset, pointer section has two entries
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,

        // List pointer, offset of one, inline composite, two words long
        // (The list only needs to be one word long to hold its actual elements;
        // therefore this message is not canonical.)
        0x05, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,

        // Struct pointer, offset two, data section has one word
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        // Tag word, struct, one element, one word data section
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,

        // Data section of struct element of list
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,

        // Data section of struct field in top-level struct
        0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}

#[test]
fn truncate_data_section_inline_composite() {
    let segment: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x4a, 0x00, 0x00, 0x00,
        0x35, 0x35, 0x35, 0x2d, 0x31, 0x32, 0x31, 0x32,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();

    let canonical_segments = &[&canonical_words[..]];
    let canonical_segment_array = message::SegmentArray::new(canonical_segments);
    let canonical_message = message::Reader::new(canonical_segment_array, Default::default());
    assert!(canonical_message.is_canonical().unwrap());
}

#[test]
fn truncate_pointer_section_inline_composite() {
    let segment: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();

    let canonical_segments = &[&canonical_words[..]];
    let canonical_segment_array = message::SegmentArray::new(canonical_segments);
    let canonical_message = message::Reader::new(canonical_segment_array, Default::default());
    assert!(canonical_message.is_canonical().unwrap());

    let expected_canonical_words: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
    ];

    assert_eq!(expected_canonical_words, &canonical_words[..]);
}

#[test]
fn list_padding_must_be_zero() {
    let segment: &[u8] = &[
        // List of three single-byte elements
        0x01, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,

        // Fourth byte is also nonzero, so this list is not canonical
        0x01, 0x02, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();

    let canonical_segments = &[&canonical_words[..]];
    let canonical_segment_array = message::SegmentArray::new(canonical_segments);
    let canonical_message = message::Reader::new(canonical_segment_array, Default::default());
    assert!(canonical_message.is_canonical().unwrap());


    let expected_canonical_words: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,
        0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    assert_eq!(expected_canonical_words, &canonical_words[..]);
}

#[test]
fn bit_list_padding_must_be_zero() {
    let segment: &[u8] = &[
        // List of eleven single-bit elements
        0x01, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00,

        // Twelfth bit is nonzero, so list is not canonical
        0xee, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());

    let canonical_words = message.canonicalize().unwrap();

    let canonical_segments = &[&canonical_words[..]];
    let canonical_segment_array = message::SegmentArray::new(canonical_segments);
    let canonical_message = message::Reader::new(canonical_segment_array, Default::default());
    assert!(canonical_message.is_canonical().unwrap());


    let expected_canonical_words: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00,
        0xee, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    assert_eq!(expected_canonical_words, &canonical_words[..]);
}

#[test]
fn out_of_bounds_zero_sized_list_returns_error() {
    let segment: &[u8] = &[
        // List pointer, offset out of bounds, elements are byte-sized, zero elements.
        0x01, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().is_err());
}

#[test]
fn out_of_bounds_zero_sized_void_list_returns_error() {
    let segment: &[u8] = &[
        // List pointer, offset out of bounds, elements have size zero, two elements.
        0x01, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(message.is_canonical().is_err());
}

#[test]
fn far_pointer_to_same_segment() {
    let segment: &[u8] = &[
        // Far pointer to this same segment. Landing pad is two words, offset of one.
        0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

        // Landing pad. Again, points back to this same segment.
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

        // Tag word, describing struct with 2-word data section.
        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    ];

    let segments = &[segment];
    let segment_array = message::SegmentArray::new(segments);
    let message = message::Reader::new(segment_array, Default::default());
    assert!(!message.is_canonical().unwrap());
}
