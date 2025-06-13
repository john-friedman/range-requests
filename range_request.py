import urllib.request
import json
import tarfile
import io
import time

def retrieve_documents_with_range(url, document_type):
    """Retrieve specific documents using range requests"""
    if isinstance(document_type, str):
        document_type = [document_type]
    
    # Request first 64KB for metadata
    req = urllib.request.Request(url)
    req.add_header('Range', 'bytes=0-65535')
    with urllib.request.urlopen(req) as response:
        initial_data = response.read()
    
    # Extract metadata.json
    tar_buffer = io.BytesIO(initial_data)
    with tarfile.open(fileobj=tar_buffer, mode='r|') as tar:
        for member in tar:
            if member.name == 'metadata.json':
                metadata_file = tar.extractfile(member)
                metadata = json.loads(metadata_file.read().decode('utf-8'))
                break

    # Find target documents
    results = []
    for doc in metadata['documents']:
        if doc['type'] in document_type:
            # Range request for specific document
            req = urllib.request.Request(url)
            req.add_header('Range', f"bytes={doc['secsgml_start_byte']}-{doc['secsgml_end_byte']}")
            with urllib.request.urlopen(req) as response:
                content = response.read().decode('utf-8', errors='ignore')
                results.append(content)
    
    return results

def retrieve_documents_full_download(url, document_type):
    """Retrieve specific documents by downloading full tar file"""
    if isinstance(document_type, str):
        document_type = [document_type]
    
    # Download entire tar file
    with urllib.request.urlopen(url) as response:
        tar_data = response.read()
    
    # Extract metadata and documents
    tar_buffer = io.BytesIO(tar_data)
    metadata = None
    results = []
    
    with tarfile.open(fileobj=tar_buffer, mode='r') as tar:
        # First get metadata
        metadata_member = tar.getmember('metadata.json')
        metadata_file = tar.extractfile(metadata_member)
        metadata = json.loads(metadata_file.read().decode('utf-8'))
        
        # Find and extract target documents
        for doc in metadata['documents']:
            if doc['type'] in document_type:
                # Find the document in tar
                for member in tar.getmembers():
                    if member.name == doc['filename']:
                        doc_file = tar.extractfile(member)
                        content = doc_file.read().decode('utf-8', errors='ignore')
                        results.append(content)
                        break
    
    return results

def run_comparison_test():
    """Run timing comparison between range requests and full download"""
    url = "https://raw.githubusercontent.com/john-friedman/range-requests/refs/heads/master/msft/000103221002001353.tar"
    document_type = '8-K'
    
    print("=== Range Request vs Full Download Comparison ===\n")
    
    # Test 1: Range Request Method
    print("Testing Range Request Method...")
    start_time = time.time()
    try:
        range_results = retrieve_documents_with_range(url, document_type)
        range_time = time.time() - start_time
        range_success = True
        range_size = len(range_results[0]) if range_results else 0
    except Exception as e:
        range_time = time.time() - start_time
        range_success = False
        range_size = 0
        print(f"Range request failed: {e}")
    
    # Test 2: Full Download Method
    print("Testing Full Download Method...")
    start_time = time.time()
    try:
        full_results = retrieve_documents_full_download(url, document_type)
        full_time = time.time() - start_time
        full_success = True
        full_size = len(full_results[0]) if full_results else 0
    except Exception as e:
        full_time = time.time() - start_time
        full_success = False
        full_size = 0
        print(f"Full download failed: {e}")
    
    # Print results
    print("\n=== RESULTS ===")
    print(f"Range Request Method:")
    print(f"  Time: {range_time:.3f} seconds")
    print(f"  Success: {range_success}")
    print(f"  Document size: {range_size:,} characters")
    
    print(f"\nFull Download Method:")
    print(f"  Time: {full_time:.3f} seconds")
    print(f"  Success: {full_success}")
    print(f"  Document size: {full_size:,} characters")
    
    if range_success and full_success:
        speedup = full_time / range_time
        print(f"\nSpeedup: {speedup:.2f}x faster with range requests")
        print(f"Time saved: {full_time - range_time:.3f} seconds")
        
        # Verify content is the same
        content_match = range_results[0] == full_results[0]
        print(f"Content matches: {content_match}")
    
    print("\n" + "="*50)

def run_multiple_tests(num_tests=3):
    """Run multiple tests and average the results"""
    url = "https://raw.githubusercontent.com/john-friedman/range-requests/refs/heads/master/msft/000103221002001353.tar"
    document_type = 'EX-99.1'
    
    print(f"=== Running {num_tests} Tests for Average Performance ===\n")
    
    range_times = []
    full_times = []
    
    for i in range(num_tests):
        print(f"Test {i+1}/{num_tests}")
        
        # Range request test
        start_time = time.time()
        try:
            range_results = retrieve_documents_with_range(url, document_type)
            range_time = time.time() - start_time
            range_times.append(range_time)
        except Exception as e:
            print(f"Range request failed in test {i+1}: {e}")
            continue
        
        # Full download test
        start_time = time.time()
        try:
            full_results = retrieve_documents_full_download(url, document_type)
            full_time = time.time() - start_time
            full_times.append(full_time)
        except Exception as e:
            print(f"Full download failed in test {i+1}: {e}")
            continue
    
    if range_times and full_times:
        avg_range_time = sum(range_times) / len(range_times)
        avg_full_time = sum(full_times) / len(full_times)
        
        print(f"\n=== AVERAGE RESULTS ===")
        print(f"Range Request Average: {avg_range_time:.3f} seconds")
        print(f"Full Download Average: {avg_full_time:.3f} seconds")
        print(f"Average Speedup: {avg_full_time / avg_range_time:.2f}x")
        print(f"Average Time Saved: {avg_full_time - avg_range_time:.3f} seconds")

if __name__ == "__main__":
    # Run single comparison
    run_comparison_test()
    
    print("\n")
    
    # Run multiple tests for average
    run_multiple_tests(3)