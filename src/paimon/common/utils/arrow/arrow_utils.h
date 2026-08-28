/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/type_fwd.h"
#include "paimon/result.h"

struct ArrowArray;

namespace paimon {

class PAIMON_EXPORT ArrowUtils {
 public:
    ArrowUtils() = delete;
    ~ArrowUtils() = delete;

    static const char* kArrowSchemaMetadataKey;

    static Result<std::shared_ptr<arrow::Schema>> DataTypeToSchema(
        const std::shared_ptr<arrow::DataType>& data_type);

    static Result<std::vector<int32_t>> CreateProjection(
        const std::shared_ptr<arrow::Schema>& src_schema, const arrow::FieldVector& target_fields);

    static Status CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::shared_ptr<arrow::Array>& data);

    // For struct array, arrow is unsafe for fields() and field(); for dict array, arrow is unsafe
    // for dictionary(). Therefore, access array in advance before merge sort and projection to
    // avoid subsequent multi-threading problems.
    static void TraverseArray(const std::shared_ptr<arrow::Array>& array);

    static uint64_t GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data);

    static Result<std::shared_ptr<arrow::StructArray>> RemoveFieldFromStructArray(
        const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name);

    /// Returns a RecordBatch whose columns, including their nested children, all have a zero
    /// offset, as required by `BatchReader`. Offsets are rebased by slicing buffers (zero copy)
    /// wherever the layout allows it; only layouts that cannot be rebased fall back to a full copy.
    static Result<std::shared_ptr<arrow::RecordBatch>> NormalizeRecordBatchOffsets(
        const std::shared_ptr<arrow::RecordBatch>& record_batch, arrow::MemoryPool* pool);

    /// Returns an Array with zero offsets. Struct children are also sliced to the parent's visible
    /// range so the result can be exported and imported as a RecordBatch.
    static Result<std::shared_ptr<arrow::Array>> NormalizeArrayOffsets(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool);

    static bool EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                     const std::shared_ptr<arrow::DataType>& other_type);

    /// Normalize and resolve a compression string to an Arrow compression type.
    /// Handles "none" and empty string by mapping them to "uncompressed".
    static Result<arrow::Compression::type> GetCompressionType(const std::string& compression);

    /// Whether a column of `type` may be carried dictionary-encoded across the Arrow C data
    /// interface, which drops the type and leaves only the layout behind.
    ///
    /// A layout pins down neither the index width nor the offset width, so the only encoding worth
    /// carrying is the one a single known producer emits: `dictionary(int32(), utf8()|binary())`,
    /// which is what Arrow's Parquet reader produces for
    /// `ArrowReaderProperties::set_read_dictionary`. `LARGE_STRING` is deliberately excluded even
    /// though it is binary-like: the ORC reader widens strings to
    /// `dictionary(int64(), large_utf8())` under lazy decoding, and reading that back as `int32`
    /// indices over `int32` offsets would silently reinterpret both buffers instead of failing.
    ///
    /// This narrows what may be carried; it cannot verify what was. See
    /// ResolveParquetDictionaryStructType() for where the index width becomes a caller contract.
    ///
    /// This is the single definition shared by the reader that decides which columns to request
    /// encoded and by the writer that has to recognise them again on the other side.
    ///
    /// @param type The column's value type, not its dictionary type.
    /// @return True when `dictionary(int32(), type)` round-trips through an `ArrowArray`.
    static bool IsParquetDictionaryValueType(const arrow::DataType& type);

    /// Recovers the struct type of a batch that Arrow's Parquet reader produced with
    /// `set_read_dictionary` enabled: `logical_type` with every top-level field whose matching
    /// child in `batch` carries a dictionary replaced by `dictionary(int32(), field type)`, or
    /// `logical_type` itself when no child is dictionary-encoded.
    ///
    /// The `int32` index width is not inferred, it is assumed, and that assumption is only valid
    /// for Arrow's Parquet reader. **The value type check does not make it safe for anything
    /// else**: it rejects `dictionary(int64(), large_utf8())`, which is the shape the ORC reader
    /// produces, but nothing here can tell `dictionary(int32(), utf8())` apart from
    /// `dictionary(int64(), utf8())`, and the second would be read as the first.
    ///
    /// So this is a contract, not a check, and it binds the code that *produces* the batch rather
    /// than the two places that call this. A producer must either be handing on a batch that came
    /// straight from Arrow's Parquet reader, or must run FlattenUnresolvableDictionaries() while
    /// the type is still known - that one does test the index width, and decodes every column this
    /// cannot resolve while leaving the rest encoded.
    /// `AppendOnlyFileStoreWrite::CompactRewrite` is today's only production path that can hand
    /// over a batch whose dictionaries the schema does not declare, and it takes the second route.
    /// The callers themselves - `ParquetFormatWriter::ResolveBatchSchema` and
    /// `DataFileWriterBase::AddFileIndexBatch` - are downstream of it and see only the layout.
    ///
    /// The value-type rejection and the rejection of a dictionary below the top level narrow the
    /// blast radius; they do not close it. Closing it needs the real `ArrowSchema` to reach the
    /// writer, which the `FormatWriter::AddBatch(ArrowArray*)` signature currently drops.
    ///
    /// A field that already carries a dictionary type is left alone: `logical_type` then comes
    /// from a caller that declared the encoding up front and already describes the batch.
    ///
    /// @param logical_type The struct type the caller declares for the batch. Returned unchanged
    ///                     when it is not a struct or its field count does not match `batch`,
    ///                     leaving the mismatch to the import's own diagnostics.
    /// @param batch Only its structure is inspected, never its data, and it is not consumed.
    /// @return `logical_type` or a copy of it carrying the recovered dictionary fields, or
    ///         NotImplemented for a dictionary this cannot describe.
    static Result<std::shared_ptr<arrow::DataType>> ResolveParquetDictionaryStructType(
        const std::shared_ptr<arrow::DataType>& logical_type, const ::ArrowArray* batch);

    /// Returns `batch` with every top-level column that ResolveParquetDictionaryStructType() could
    /// not resolve decoded to the type its field carries in `logical_type`. A column it can
    /// resolve stays dictionary-encoded, so one column that has to be decoded does not cost the
    /// others their encoding, and a batch that needs no decoding is returned unchanged.
    ///
    /// This is the counterpart of the restriction above: exporting an array through the C data
    /// interface drops its type, so a column whose encoding does not survive that round trip has
    /// to be decoded while the type is still known.
    ///
    /// @param batch The batch to decode, matched to `logical_type` by field name; a column with no
    ///              matching field is left alone.
    /// @param logical_type The struct type the decoded columns are cast to. `batch` is returned
    ///                     unchanged when it is not a struct.
    /// @param pool Allocates the decoded columns. Only used when a column is actually decoded.
    /// @return `batch` itself when nothing had to be decoded, otherwise a copy of it with the
    ///         offset, length and validity of the original and the decoded columns swapped in.
    static Result<std::shared_ptr<arrow::StructArray>> FlattenUnresolvableDictionaries(
        const std::shared_ptr<arrow::StructArray>& batch,
        const std::shared_ptr<arrow::DataType>& logical_type, arrow::MemoryPool* pool);

 private:
    static Status InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                             const std::shared_ptr<arrow::Array>& data);
};

}  // namespace paimon
