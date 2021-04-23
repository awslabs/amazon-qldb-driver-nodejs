/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

import { createHash } from "crypto";
import { cryptoHasherProvider, HashReader, makeHashReader } from "ion-hash-js";
import { makeReader, makeTextWriter, Writer } from "ion-js";

const HASH_SIZE: number = 32

/**
 * A QLDB hash is either a 256 bit number or a special empty hash.
 * 
 * @internal
 */
export class QldbHash {
    private _qldbHash: Uint8Array;

    /**
     * Creates a QldbHash.
     * @param qldbHash The QLDB hash.
     * @throws RangeError When this hash is not the correct hash size.
     */
    constructor(qldbHash: Uint8Array) {
        if (qldbHash.length !== HASH_SIZE || qldbHash.length === 0) {
            throw new RangeError(`Hash must be either empty or ${HASH_SIZE} bytes long.`);
        }
        this._qldbHash = qldbHash;
    }

    /**
     * Sort the current hash value and the hash value provided by `that`, comparing by their **signed** byte values in
     * little-endian order.
     * @param that The Ion hash of the Ion value to compare.
     * @returns An QldbHash object that contains the concatenated hash values.
     */
    dot(that: QldbHash): QldbHash {
        const concatenated: Uint8Array = QldbHash._joinHashesPairwise(this.getQldbHash(), that.getQldbHash());
        const newHashLib = createHash("sha256");
        newHashLib.update(concatenated);
        const newDigest: Uint8Array = newHashLib.digest();
        return new QldbHash(newDigest);
    }

    equals(other: QldbHash): boolean {
        return (QldbHash._hashComparator(this.getQldbHash(), other.getQldbHash()) === 0);
    }

    getHashSize(): number {
        return this._qldbHash.length;
    }

    getQldbHash(): Uint8Array {
        return this._qldbHash;
    }

    isEmpty(): boolean {
        return (this._qldbHash.length === 0);
    }

    /**
     * The QldbHash of an IonValue is just the IonHash of that value.
     * @param value The string or Ion value to be converted to Ion hash.
     * @returns A QldbHash object that contains Ion hash.
     */
    static toQldbHash(value: any): QldbHash {
        if (typeof value === "string") {
            const writer: Writer = makeTextWriter();
            writer.writeString(value);
            writer.close();
            value = writer.getBytes();
        }
        const hashReader: HashReader = makeHashReader(makeReader(value), cryptoHasherProvider("sha256"));
        hashReader.next();
        hashReader.next();
        const digest: Uint8Array = hashReader.digest();
        return new QldbHash(digest);
    }

    /**
     * Helper method that concatenates two Uint8Array.
     * @param arrays List of arrays to concatenate, in the order provided.
     * @returns The concatenated array.
     */
    static _concatenate(...arrays: Uint8Array[]): Uint8Array {
        let totalLength = 0;
        for (const arr of arrays) {
            totalLength += arr.length;
        }
        const result = new Uint8Array(totalLength);
        let offset = 0;
        for (const arr of arrays) {
            result.set(arr, offset);
            offset += arr.length;
        }
        return result;
    }

    /**
     * Compares two hashes by their **signed** byte values in little-endian order.
     * @param hash1 The hash value to compare.
     * @param hash2 The hash value to compare.
     * @returns Zero if the hash values are equal, otherwise return the difference of the first pair of non-matching
     *          bytes.
     * @throws RangeError When the hash is not the correct hash size.
     */
    static _hashComparator(hash1: Uint8Array, hash2: Uint8Array): number {
        if (hash1.length !== HASH_SIZE || hash2.length !== HASH_SIZE) {
            throw new RangeError("Invalid hash.");
        }
        for (let i = hash1.length-1; i >= 0; i--) {
            const difference: number = (hash1[i]<<24 >>24) - (hash2[i]<<24 >>24);
            if (difference !== 0) {
                return difference;
            }
        }
        return 0;
    }

    /**
     * Takes two hashes, sorts them, and concatenates them.
     * @param h1 Byte array containing one of the hashes to compare.
     * @param h2 Byte array containing one of the hashes to compare.
     * @returns The concatenated array of hashes.
     */
    static _joinHashesPairwise(h1: Uint8Array, h2: Uint8Array): Uint8Array {
        if (h1.length === 0) {
            return h2;
        }
        if (h2.length === 0) {
            return h1;
        }
        let concatenated: Uint8Array;
        if (this._hashComparator(h1, h2) < 0) {
            concatenated = this._concatenate(h1, h2);
        } else {
            concatenated = this._concatenate(h2, h1);
        }
        return concatenated;
    }
}
