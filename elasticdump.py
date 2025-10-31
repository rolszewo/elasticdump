#!/usr/bin/env python3
"""
Parallel Elasticsearch Index Export to NDJSON

Key Features:

✅ Auto-calculated optimal slices based on document count
✅ Pattern matching for bulk exports (logs-*, prod-*, etc.)
✅ Environment variable support (ES_URL, ES_USERNAME, ES_PASSWORD)
✅ Fast binary gzip concatenation (10-100x faster)
✅ Handles small indices (no slice API errors)
✅ Per-index combined files with --combine
✅ Comprehensive statistics and verification
"""
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json
import gzip
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import urllib3
import getpass
import time
from datetime import timedelta
import multiprocessing
import shutil
import fnmatch

urllib3.disable_warnings()

def get_es_client(es_url, username=None, password=None):
    """Create Elasticsearch client"""
    if username and password:
        return Elasticsearch(
            [es_url],
            verify_certs=False,
            basic_auth=(username, password),
            request_timeout=30
        )
    else:
        return Elasticsearch(
            [es_url],
            verify_certs=False,
            request_timeout=30
        )

def list_indices(es_url, username=None, password=None):
    """Get list of all indices from Elasticsearch"""
    try:
        es = get_es_client(es_url, username, password)
        
        # Get all indices (excluding system indices starting with .)
        indices = es.cat.indices(format='json', h='index')
        
        # Filter out system indices and return sorted list
        index_names = [idx['index'] for idx in indices if not idx['index'].startswith('.')]
        return sorted(index_names)
    
    except Exception as e:
        print(f"⚠️  Could not list indices: {e}")
        return []

def match_indices(pattern, es_url, username=None, password=None):
    """
    Match indices based on pattern.
    If pattern contains *, use wildcard matching.
    Otherwise, match exact name.
    """
    all_indices = list_indices(es_url, username, password)
    
    if not all_indices:
        return []
    
    # Exact match if no wildcard
    if '*' not in pattern:
        if pattern in all_indices:
            return [pattern]
        else:
            return []
    
    # Wildcard matching
    matched = fnmatch.filter(all_indices, pattern)
    return sorted(matched)

def get_index_count(es_url, index, username=None, password=None):
    """Get total document count from Elasticsearch index"""
    try:
        es = get_es_client(es_url, username, password)
        result = es.count(index=index)
        return result['count']
    except Exception as e:
        print(f"⚠️  Could not get index count: {e}")
        return None

def calculate_optimal_slices(doc_count, max_workers=None):
    """
    Calculate optimal number of slices based on document count and CPU cores
    
    Rules:
    - Minimum: 2 slices (ES slice API requires max >= 2)
    - Maximum: 2x CPU cores (to avoid overwhelming the system)
    - Target: ~100k-500k documents per slice for good balance
    - Round to reasonable numbers (powers of 2 or multiples of CPU cores)
    """
    if max_workers is None:
        cpu_cores = multiprocessing.cpu_count()
        max_workers = cpu_cores * 2  # Allow up to 2x CPU cores
    
    if doc_count is None or doc_count == 0:
        return min(4, max_workers)  # Default to 4 if count unknown
    
    # For very small indices, use single slice (no slicing)
    if doc_count < 10000:
        return 1
    
    # Target 100k-500k docs per slice, prefer middle ground (250k)
    target_docs_per_slice = 250_000
    calculated_slices = max(2, doc_count // target_docs_per_slice)  # Min 2 for slice API
    
    # Cap at max_workers
    optimal_slices = min(calculated_slices, max_workers)
    
    # Round to nice numbers for better distribution
    if optimal_slices <= 4:
        return optimal_slices
    elif optimal_slices <= 8:
        return 8
    elif optimal_slices <= 16:
        return 16
    elif optimal_slices <= 32:
        return 32
    else:
        # Round to nearest multiple of 8 for large counts
        return ((optimal_slices + 7) // 8) * 8

def export_slice(es_url, index, slice_id, max_slices, output_dir, username=None, password=None):
    """Export a single slice of the index"""
    
    # Create ES connection for this thread
    if username and password:
        es = Elasticsearch(
            [es_url],
            verify_certs=False,
            basic_auth=(username, password),
            request_timeout=300,
            max_retries=10,
            retry_on_timeout=True
        )
    else:
        es = Elasticsearch(
            [es_url],
            verify_certs=False,
            request_timeout=300,
            max_retries=10,
            retry_on_timeout=True
        )
    
    filename = os.path.join(output_dir, f'slice_{slice_id:04d}.ndjson.gz')
    doc_count = 0
    
    try:
        # Build query based on whether we're using slicing
        if max_slices == 1:
            # Single slice - don't use slice API (ES requires max >= 2)
            query = {"query": {"match_all": {}}}
        else:
            # Multiple slices - use slice API
            query = {
                "slice": {"id": slice_id, "max": max_slices},
                "query": {"match_all": {}}
            }
        
        with gzip.open(filename, 'wt', encoding='utf-8') as f:
            for doc in scan(
                es,
                index=index,
                query=query,
                scroll='5m',
                size=5000,
                raise_on_error=False,
                preserve_order=False
            ):
                f.write(json.dumps(doc['_source']) + '\n')
                doc_count += 1
        
        return slice_id, doc_count, None
    
    except Exception as e:
        return slice_id, 0, str(e)

def combine_gzip_files_fast(slice_files, output_file):
    """
    Combine gzip files by concatenating them directly (binary mode).
    This is much faster than decompressing and recompressing.
    Gzip format supports concatenation - the result is still a valid gzip file.
    """
    with open(output_file, 'wb') as outfile:
        for slice_file in slice_files:
            with open(slice_file, 'rb') as infile:
                # Copy in chunks for memory efficiency
                shutil.copyfileobj(infile, outfile, length=1024*1024)  # 1MB chunks

def format_time(seconds):
    """Format seconds to human-readable string"""
    return str(timedelta(seconds=int(seconds)))

def export_single_index(index_name, args, overall_stats):
    """Export a single index"""
    
    print(f"\n{'='*70}")
    print(f"Processing Index: {index_name}")
    print(f"{'='*70}")
    
    # Create output directory for this index
    output_dir = os.path.join(args.output, index_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # Get document count
    print("Checking index document count...")
    es_doc_count = get_index_count(args.url, index_name, args.username, args.password)
    
    # Calculate optimal slices if not specified
    if args.slices is None:
        num_slices = calculate_optimal_slices(es_doc_count)
        slice_mode = "auto"
    else:
        num_slices = args.slices
        slice_mode = "manual"
    
    if es_doc_count is not None:
        if num_slices == 1:
            print(f"✅ Documents: {es_doc_count:,} | Slices: 1 (single export, no slicing)")
        else:
            print(f"✅ Documents: {es_doc_count:,} | Slices: {num_slices} ({slice_mode})")
            print(f"   → ~{es_doc_count//num_slices:,} docs per slice")
    else:
        print(f"⚠️  Could not retrieve count | Slices: {num_slices} (default)")
    
    # Start timer for this index
    start_time = time.time()
    
    # Export slices in parallel
    total_docs = 0
    failed_slices = []
    
    print(f"\nExporting {index_name}:")
    
    with ThreadPoolExecutor(max_workers=num_slices) as executor:
        futures = [
            executor.submit(
                export_slice,
                args.url,
                index_name,
                i,
                num_slices,
                output_dir,
                args.username,
                args.password
            )
            for i in range(num_slices)
        ]
        
        # Progress bar
        for future in tqdm(as_completed(futures), total=num_slices, desc="  Progress", unit="slice"):
            slice_id, doc_count, error = future.result()
            
            if error:
                tqdm.write(f"  ❌ Slice {slice_id} failed: {error}")
                failed_slices.append(slice_id)
            else:
                total_docs += doc_count
                if num_slices == 1:
                    tqdm.write(f"  ✅ Exported: {doc_count:,} documents")
                else:
                    tqdm.write(f"  ✅ Slice {slice_id}: {doc_count:,} documents")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    docs_per_second = total_docs / elapsed_time if elapsed_time > 0 else 0
    
    # Print index summary
    print(f"\n  {'─'*66}")
    print(f"  Index Summary: {index_name}")
    print(f"  {'─'*66}")
    print(f"  Expected:        {es_doc_count:,}" if es_doc_count else "  Expected:        Unknown")
    print(f"  Exported:        {total_docs:,}")
    
    # Verification
    status = "✅"
    if failed_slices:
        status = "❌"
    elif es_doc_count is not None and total_docs != es_doc_count:
        status = "⚠️"
    
    print(f"  Status:          {status}")
    print(f"  Failed slices:   {len(failed_slices)}")
    print(f"  Time:            {format_time(elapsed_time)}")
    print(f"  Speed:           {docs_per_second:,.0f} docs/sec")
    print(f"  Output:          {output_dir}/slice_*.ndjson.gz")
    
    # Combine files if requested
    combined_file = None
    if args.combine and not failed_slices and total_docs > 0:
        print(f"\n  Combining {num_slices} file(s)...")
        combine_start = time.time()
        
        combined_file = os.path.join(args.output, f"{index_name}.ndjson.gz")
        
        # Get list of slice files in order
        slice_files = [
            os.path.join(output_dir, f'slice_{i:04d}.ndjson.gz')
            for i in range(num_slices)
        ]
        
        # Fast binary concatenation
        for i, slice_file in enumerate(slice_files):
            if i == 0:
                shutil.copy2(slice_file, combined_file)
            else:
                with open(combined_file, 'ab') as outfile:
                    with open(slice_file, 'rb') as infile:
                        shutil.copyfileobj(infile, outfile, length=1024*1024)
        
        combine_time = time.time() - combine_start
        file_size_mb = os.path.getsize(combined_file) / (1024 * 1024)
        
        print(f"  ✅ Combined: {index_name}.ndjson.gz ({file_size_mb:.2f} MB in {combine_time:.1f}s)")
    
    # Update overall statistics
    overall_stats['total_indices'] += 1
    overall_stats['total_docs'] += total_docs
    overall_stats['total_time'] += elapsed_time
    
    if failed_slices:
        overall_stats['failed_indices'].append(index_name)
    else:
        overall_stats['successful_indices'] += 1
    
    if es_doc_count is not None:
        overall_stats['expected_docs'] += es_doc_count
        if total_docs != es_doc_count:
            overall_stats['mismatched_indices'].append(index_name)
    
    return {
        'index': index_name,
        'expected': es_doc_count,
        'exported': total_docs,
        'failed_slices': failed_slices,
        'time': elapsed_time,
        'combined_file': combined_file
    }

def main():
    parser = argparse.ArgumentParser(
        description='Export Elasticsearch index(es) in parallel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  ES_URL       Elasticsearch URL (default: http://localhost:9200)
  ES_USERNAME  Elasticsearch username (if auth required)
  ES_PASSWORD  Elasticsearch password (if auth required)

Index Patterns:
  Without '*' - exact match:
    --index my-index              → exports only "my-index"
  
  With '*' - wildcard match:
    --index "logs-*"              → exports logs-2024-01, logs-2024-02, etc.
    --index "prod-*"              → exports all indices starting with "prod-"
    --index "*-archive"           → exports all indices ending with "-archive"

Examples:
  # Export single index
  %(prog)s --index my-index

  # Export all matching indices with pattern
  %(prog)s --index "logs-2024-*" --combine

  # List what would be exported (dry-run)
  %(prog)s --index "prod-*" --list-only

  # With authentication
  export ES_USERNAME=elastic ES_PASSWORD=secret
  %(prog)s --index "myapp-*" --combine

  # Custom output directory
  %(prog)s --index "logs-*" --output /backup/es-export
        """
    )
    
    parser.add_argument('--url', 
                        default=None,
                        help='Elasticsearch URL (env: ES_URL, default: http://localhost:9200)')
    parser.add_argument('--index', 
                        required=True, 
                        help='Index name or pattern (use * for wildcard matching)')
    parser.add_argument('--slices', 
                        type=int, 
                        default=None,
                        help='Number of parallel slices (default: auto-calculate based on doc count)')
    parser.add_argument('--output', 
                        default='export', 
                        help='Output directory (default: export/)')
    parser.add_argument('--combine', 
                        action='store_true', 
                        help='Combine slices into single INDEXNAME.ndjson.gz file per index')
    parser.add_argument('--username', 
                        default=None,
                        help='ES username (env: ES_USERNAME)')
    parser.add_argument('--password', 
                        default=None,
                        help='ES password (env: ES_PASSWORD)')
    parser.add_argument('--list-only',
                        action='store_true',
                        help='Only list matching indices without exporting')
    
    args = parser.parse_args()
    
    # Get configuration from ENV variables with fallbacks
    args.url = args.url or os.environ.get('ES_URL', 'http://localhost:9200')
    args.username = args.username or os.environ.get('ES_USERNAME')
    args.password = args.password or os.environ.get('ES_PASSWORD')
    
    # Prompt for password if username provided but no password
    if args.username and not args.password:
        args.password = getpass.getpass(f"Password for user '{args.username}': ")
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    print(f"\n{'='*70}")
    print(f"Elasticsearch Index Export")
    print(f"{'='*70}")
    
    # Match indices based on pattern
    print(f"Matching indices for pattern: '{args.index}'")
    matched_indices = match_indices(args.index, args.url, args.username, args.password)
    
    if not matched_indices:
        print(f"\n❌ No indices found matching pattern: '{args.index}'")
        print(f"\nTip: Use --list-only to see all available indices")
        return 1
    
    print(f"✅ Found {len(matched_indices)} matching index(es):\n")
    for idx in matched_indices:
        print(f"   • {idx}")
    
    # List only mode
    if args.list_only:
        print(f"\n{'='*70}")
        print(f"List-only mode: No export performed")
        print(f"{'='*70}\n")
        return 0
    
    print(f"\n{'='*70}")
    print(f"Configuration")
    print(f"{'='*70}")
    print(f"  Pattern:             {args.index}")
    print(f"  Matched indices:     {len(matched_indices)}")
    print(f"  ES URL:              {args.url}")
    print(f"  Slices per index:    {'auto' if args.slices is None else args.slices}")
    print(f"  Output directory:    {args.output}/")
    print(f"  Authentication:      {'Yes (' + args.username + ')' if args.username else 'No'}")
    print(f"  Combine files:       {'Yes' if args.combine else 'No'}")
    print(f"{'='*70}")
    
    # Overall statistics
    overall_stats = {
        'total_indices': 0,
        'successful_indices': 0,
        'failed_indices': [],
        'mismatched_indices': [],
        'total_docs': 0,
        'expected_docs': 0,
        'total_time': 0
    }
    
    # Export each index
    overall_start = time.time()
    results = []
    
    for index_name in matched_indices:
        result = export_single_index(index_name, args, overall_stats)
        results.append(result)
    
    overall_elapsed = time.time() - overall_start
    
    # Final Summary
    print(f"\n{'='*70}")
    print(f"OVERALL SUMMARY")
    print(f"{'='*70}")
    print(f"  Total indices:       {overall_stats['total_indices']}")
    print(f"  Successful:          {overall_stats['successful_indices']}")
    print(f"  Failed:              {len(overall_stats['failed_indices'])}")
    if overall_stats['failed_indices']:
        print(f"    → {', '.join(overall_stats['failed_indices'])}")
    
    print(f"\n  Total documents:     {overall_stats['total_docs']:,}")
    if overall_stats['expected_docs'] > 0:
        print(f"  Expected documents:  {overall_stats['expected_docs']:,}")
        if overall_stats['total_docs'] == overall_stats['expected_docs']:
            print(f"  ✅ Verification:     All documents exported correctly")
        else:
            diff = abs(overall_stats['total_docs'] - overall_stats['expected_docs'])
            print(f"  ⚠️  Verification:     {diff:,} documents difference")
    
    if overall_stats['mismatched_indices']:
        print(f"\n  ⚠️  Mismatched indices: {len(overall_stats['mismatched_indices'])}")
        for idx in overall_stats['mismatched_indices']:
            print(f"    → {idx}")
    
    print(f"\n  Total time:          {format_time(overall_elapsed)}")
    if overall_stats['total_docs'] > 0 and overall_elapsed > 0:
        overall_speed = overall_stats['total_docs'] / overall_elapsed
        print(f"  Overall speed:       {overall_speed:,.0f} docs/sec")
    
    print(f"\n  Output location:     {args.output}/")
    
    if args.combine:
        print(f"\n  Combined files:")
        for result in results:
            if result['combined_file'] and os.path.exists(result['combined_file']):
                size_mb = os.path.getsize(result['combined_file']) / (1024 * 1024)
                print(f"    • {os.path.basename(result['combined_file'])} ({size_mb:.2f} MB)")
    
    print(f"{'='*70}")
    
    # Exit status
    if overall_stats['failed_indices']:
        print(f"\n⚠️  Export completed with errors")
        return 1
    elif overall_stats['mismatched_indices']:
        print(f"\n⚠️  Export completed with count mismatches")
        return 1
    else:
        print(f"\n✅ All indices exported successfully!")
        return 0

if __name__ == '__main__':
    exit(main())