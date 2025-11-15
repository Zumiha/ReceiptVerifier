import os
import requests
import json
import csv
import time
import hashlib
import logging
from typing import Dict, Optional, List, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ReceiptItem:
    """Single item in receipt"""
    name: str
    price: float  # in rubles
    quantity: float
    sum: float  # in rubles


@dataclass
class Receipt:
    """Receipt data object"""
    # Status
    code: int
    is_valid: bool
    date_time: Optional[datetime] = None
    error_message: Optional[str] = None

    # Organization info
    organization: str = ""
    address: str = ""
    inn: str = ""

    # Receipt info
    date: str = ""
    time: str = ""
    place: str = ""
    cashier: str = ""
    receipt_number: str = ""
    shift_number: str = ""

    # Fiscal info
    fiscal_drive_number: str = ""
    fiscal_document_number: str = ""
    fiscal_sign: str = ""
    operation_type: int = 0

    # Items
    items: List[ReceiptItem] = field(default_factory=list)

    # Totals
    total_sum: float = 0.0  # in rubles
    cash_sum: float = 0.0
    card_sum: float = 0.0

    # VAT
    vat_20: float = 0.0
    vat_10: float = 0.0
    vat_0: float = 0.0
    vat_none: float = 0.0

    # Raw response
    raw_response: Dict = field(default_factory=dict)
    html_content: str = ""

    @classmethod
    def from_api_response(cls, response: Dict) -> 'Receipt':
        """Create Receipt object from API response"""
        code = response.get('code', -1)

        # Handle error codes
        error_messages = {
            0: "Invalid receipt",
            2: "Receipt data not yet available",
            3: "Request limit exceeded",
            4: "Wait before retrying",
            5: "Data not received"
        }

        if code in error_messages:
            return cls(
                code=code,
                is_valid=False,
                error_message=error_messages[code],
                date_time=datetime.now()
            )
        elif code != 1:
            error = response.get('error', 'Unknown error')
            return cls(
                code=code,
                is_valid=False,
                error_message=error,
                date_time=datetime.now()
            )

        # Parse successful response
        data = response.get('data', {}).get('json', {})

        # Parse date
        date_str = data.get('dateTime', '')
        try:
            date_time = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S') if date_str else None
        except ValueError:
            date_time = None

        # Parse items
        items = []
        for item_data in data.get('items', []):
            items.append(ReceiptItem(
                name=item_data.get('name', ''),
                price=item_data.get('price', 0) / 100,
                quantity=item_data.get('quantity', 0),
                sum=item_data.get('sum', 0) / 100
            ))

        return cls(
            code=code,
            is_valid=True,
            organization=data.get('user', ''),
            address=data.get('retailPlaceAddress', ''),
            inn=data.get('userInn', ''),
            time=date_time.strftime('%H:%M:%S'),
            date=date_time.strftime('%Y-%m-%d'),
            place=data.get('retailPlace', ''),
            cashier=data.get('operator', ''),
            receipt_number=data.get('requestNumber', ''),
            shift_number=data.get('shiftNumber', ''),
            fiscal_drive_number=data.get('fiscalDriveNumber', ''),
            fiscal_document_number=data.get('fiscalDocumentNumber', ''),
            fiscal_sign=data.get('fiscalSign', ''),
            operation_type=data.get('operationType', 0),
            items=items,
            total_sum=data.get('totalSum', 0) / 100,
            cash_sum=data.get('cashTotalSum', 0) / 100,
            card_sum=data.get('ecashTotalSum', 0) / 100,
            vat_20=data.get('nds18', 0) / 100,
            vat_10=data.get('nds', 0) / 100,
            vat_0=data.get('nds0', 0) / 100,
            vat_none=data.get('ndsNo', 0) / 100,
            html_content=response.get('data', {}).get('html', ''),
            raw_response=response
        )

    def to_text(self) -> str:
        """Format receipt as readable text"""
        if not self.is_valid:
            return f"❌ {self.error_message}"

        result = f"✅ Receipt verified\n\n"
        result += f"Organization: {self.organization}\n"
        result += f"Address: {self.address}\n"
        result += f"INN: {self.inn}\n"
        result += f"Date: {self.date}\n"
        result += f"Total: {self.total_sum:.2f} ₽\n"
        result += f"Cash: {self.cash_sum:.2f} ₽\n"
        result += f"Card: {self.card_sum:.2f} ₽\n\n"

        if self.items:
            result += "Items:\n"
            for item in self.items:
                result += f"  • {item.name} - {item.price:.2f} ₽ x {item.quantity}\n"

        return result

    def to_csv(self, filename: Optional[str] = None, receipts_dir: str = "receipts") -> Optional[str]:
        """Save receipt to CSV file"""
        if not self.is_valid:
            return None
        
        # Create receipts directory if it doesn't exist
        Path(receipts_dir).mkdir(exist_ok=True)

        # Generate filename if not provided
        if filename is None:
            filename = f"{self.date}_{self.time.replace(':', '-')}_{self.fiscal_drive_number}_{self.fiscal_document_number}_{self.fiscal_sign}.csv"
            filename = os.path.join(receipts_dir, filename)

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                # Header info
                writer.writerow(['Receipt Information'])
                writer.writerow(['Organization', self.organization])
                writer.writerow(['Place', self.place])                
                writer.writerow(['Address', self.address])
                writer.writerow(['INN', self.inn])
                writer.writerow(['Date', self.date])
                writer.writerow(['Time', self.time])
                writer.writerow(['Cashier', self.cashier])
                writer.writerow(['Receipt Number', self.receipt_number])
                writer.writerow(['Shift', self.shift_number])
                writer.writerow(['Fiscal Drive Number', self.fiscal_drive_number])
                writer.writerow(['Fiscal Document Number', self.fiscal_document_number])
                writer.writerow(['Fiscal Sign', self.fiscal_sign])
                writer.writerow([])

                # Items
                writer.writerow(['Item Name', 'Price (₽)', 'Quantity', 'Sum (₽)'])
                for item in self.items:
                    writer.writerow([item.name, f"{item.price:.2f}", item.quantity, f"{item.sum:.2f}"])

                writer.writerow([])

                # Totals
                writer.writerow(['Payment Method', 'Amount (₽)'])
                writer.writerow(['Cash', f"{self.cash_sum:.2f}"])
                writer.writerow(['Card', f"{self.card_sum:.2f}"])
                writer.writerow(['TOTAL', f"{self.total_sum:.2f}"])

                # VAT
                writer.writerow([])
                writer.writerow(['VAT Information'])
                writer.writerow(['VAT 20%', f"{self.vat_20:.2f}"])
                writer.writerow(['VAT 10%', f"{self.vat_10:.2f}"])
                writer.writerow(['VAT 0%', f"{self.vat_0:.2f}"])
                writer.writerow(['No VAT', f"{self.vat_none:.2f}"])

            return filename

        except Exception as e:
            print(f"Error saving CSV: {e}")
            return None


@dataclass
class RequestParams:
    """Structured request parameters with validation"""
    fn: str  # Fiscal drive number
    fd: str  # Fiscal document number
    fp: str  # Fiscal sign
    t: str  # Date time (format: YYYYMMDDTHHmm)
    n: str  # Operation type (1-4)
    s: str  # Total sum in rubles
    qr: str = "0"  # QR scan flag

    def to_dict(self) -> Dict[str, str]:
        """Convert to API request dict"""
        return {
            'fn': self.fn,
            'fd': self.fd,
            'fp': self.fp,
            't': self.t,
            'n': self.n,
            's': self.s,
            'qr': self.qr
        }

    def fingerprint(self) -> str:
        """Generate unique fingerprint for caching"""
        key = f"{self.fn}_{self.fd}_{self.fp}_{self.t}"
        return hashlib.md5(key.encode()).hexdigest()


class RequestBuilder:
    """
    Factory for creating API requests from multiple input formats.
    Supports: manual parameters, QR raw string, QR image URL, QR image file.
    """

    @staticmethod
    def from_manual_params(data: Dict[str, str]) -> Dict[str, str]:
        """Create request from manual fiscal parameters"""
        return data

    @staticmethod
    def from_qr_string(qr_string: str) -> Dict[str, str]:
        """
        Parse QR code string into request parameters.
        Format: t=20200924T1837&s=349.93&fn=9282440300682838&i=46534&fp=1273019065&n=1
        """
        return {'qrraw': qr_string}

    @staticmethod
    def from_qr_url(url: str) -> Dict[str, str]:
        """Create request from QR image URL"""
        return {'qrurl': url}

    @staticmethod
    def from_qr_file(file_path: Union[str, Path]) -> Tuple[Dict[str, str], Dict[str, tuple]]:
        """Create request with QR image file"""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"QR image file not found: {file_path}")

        files = {'qrfile': open(file_path, 'rb')}
        data = {}
        return data, files


class RetryHandler:
    """
    Exponential backoff retry logic for API rate limits.

    Strategy:
    - Code 3 (rate limit): exponential backoff
    - Code 4 (wait): fixed delay
    - Code 2 (not ready): progressive delay
    """

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def should_retry(self, code: int) -> bool:
        """Determine if error code warrants retry"""
        return code in [2, 3, 4]  # Not ready, rate limit, wait

    def get_delay(self, code: int, attempt: int) -> float:
        """
        Calculate delay based on error code and attempt number.
        Uses exponential backoff: delay = base_delay * 2^attempt
        """
        if code == 3:  # Rate limit exceeded
            return self.base_delay * (2 ** attempt)
        elif code == 4:  # Wait before retry
            return self.base_delay * 2
        elif code == 2:  # Data not ready yet
            return self.base_delay * (1.5 ** attempt)
        return self.base_delay


class ReceiptCache:
    """
    LRU-style cache for receipt data to avoid redundant API calls.
    Uses receipt fingerprint as cache key for O(1) lookups.
    """

    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Receipt] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Receipt]:
        """Retrieve cached receipt"""
        if key in self.cache:
            self.hits += 1
            logger.debug(f"Cache hit for {key}")
            return self.cache[key]
        self.misses += 1
        return None

    def put(self, key: str, _receipt: Receipt):
        """Store receipt in cache with LRU eviction"""
        if len(self.cache) >= self.max_size:
            # Simple LRU: remove oldest item
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]
        self.cache[key] = _receipt

    def stats(self) -> Dict[str, Union[int, str]]:
        """Return cache performance statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.2f}%",
            'size': len(self.cache)
        }

    def clear(self):
        """Clear cache and reset statistics"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0


class ReceiptVerifier:
    """
    Enhanced receipt verifier with retry logic, caching, and multiple input formats.

    Features:
    - Automatic retry with exponential backoff
    - Receipt caching to minimize API calls
    - Support for all 4 API request formats
    - Optional promo_id and custom userdata parameters
    """
    def __init__(self, token: str, max_retries: int = 3, cache_size: int = 1000):
        self.token = token
        self.api_url = 'https://proverkacheka.com/api/v1/check/get'
        self.retry_handler = RetryHandler(max_retries=max_retries)
        self.cache = ReceiptCache(max_size=cache_size)

    def verify_receipt(
            self,
            request_data: Union[Dict[str, str], RequestParams],
            files: Optional[Dict] = None,
            promo_id: Optional[int] = None,
            userdata: Optional[Dict[str, str]] = None
    ) -> Receipt:
        """
        Verify receipt with automatic retry logic.

        Args:
            request_data: Request parameters (Dict for qrraw/qrurl, RequestParams for manual)
            files: Optional file dict for qrfile format
            promo_id: Optional promo campaign ID
            userdata: Optional custom parameters (userdata_<key>=value)

        Returns:
            Receipt object with verification result
        """
        # Convert RequestParams to dict if needed
        if isinstance(request_data, RequestParams):
            request_dict = request_data.to_dict()
            cache_key = request_data.fingerprint()
        else:
            request_dict = request_data.copy()
            # Generate cache key for qrraw format
            cache_key = None
            if 'qrraw' in request_dict:
                cache_key = hashlib.md5(request_dict['qrraw'].encode()).hexdigest()

        # Check cache
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached:
                return cached

        # Add token
        request_dict['token'] = self.token

        # Add optional parameters
        if promo_id:
            request_dict['promo_id'] = str(promo_id)

        if userdata:
            for key, value in userdata.items():
                request_dict[f'userdata_{key}'] = value

        # Retry loop with exponential backoff
        for attempt in range(self.retry_handler.max_retries):
            try:
                response = requests.post(self.api_url, data=request_dict, files=files)
                response.raise_for_status()
                result = response.json()

                code = result.get('code', -1)

                # Success case
                if code == 1:
                    formed_receipt = Receipt.from_api_response(result)
                    # Store receipt in cache with cache key
                    if cache_key and formed_receipt.is_valid:
                        self.cache.put(cache_key, formed_receipt)
                    return formed_receipt

                # Retry case
                if self.retry_handler.should_retry(code):
                    if attempt < self.retry_handler.max_retries - 1:
                        delay = self.retry_handler.get_delay(code, attempt)
                        logger.info(
                            f"Code {code} received, retrying in {delay:.2f}s (attempt {attempt + 1}/{self.retry_handler.max_retries})")
                        time.sleep(delay)
                        continue

                # Non-retryable error
                return Receipt.from_api_response(result)

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < self.retry_handler.max_retries - 1:
                    delay = self.retry_handler.base_delay * (2 ** attempt)
                    logger.info(f"Network error, retrying in {delay:.2f}s")
                    time.sleep(delay)
                else:
                    return Receipt(
                        code=-1,
                        is_valid=False,
                        error_message=f"Network error: {str(e)}",
                        date_time=datetime.now()
                    )
            finally:
                # Close file handles if provided
                if files:
                    for f in files.values():
                        if hasattr(f, 'close'):
                            f.close()

        return Receipt(
            code=-1,
            is_valid=False,
            error_message="Max retries exceeded",
            date_time=datetime.now()
        )

    def get_cache_stats(self) -> Dict[str, Union[int, str]]:
        """Return cache performance statistics"""
        return self.cache.stats()

    def clear_cache(self):
        """Clear the receipt cache"""
        self.cache.clear()

# Save raw response as json file
def save_json(_receipt: Receipt):
    # Specify the file path
    file_path = f"receipt_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_receipt.fiscal_drive_number}_{_receipt.fiscal_document_number}.json"
    data = _receipt.raw_response

    # Open the file in write mode ('w') and use json.dump() to write the dictionary
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4) # indent=4 for pretty-printing
    print(f"Dictionary saved to {file_path}")

def print_receipt(_receipt: Receipt):
    if _receipt.is_valid:
        print(_receipt.to_text())
        csv_file = _receipt.to_csv()
        print(f"\n✅ CSV saved to: {csv_file}")
    else:
        print(f"❌ Error: {_receipt.error_message}")        

def from_qr_string(_qr_string: str) -> Receipt:
    request = RequestBuilder.from_qr_string(_qr_string)
    return verifier.verify_receipt(request)

def from_params(data: Dict[str, str]) -> Receipt:
    request = RequestBuilder.from_manual_params(data)
    return verifier.verify_receipt(request)

def from_qr_url(_url: str) -> Receipt:
    request = RequestBuilder.from_qr_url(_url)
    return verifier.verify_receipt(request)

def from_qr_img(_file_path: str) -> Receipt:
    try:
        file_request, files = RequestBuilder.from_qr_file(_file_path)
        return verifier.verify_receipt(file_request, files=files)
    except FileNotFoundError as e:
        print(f"File error: {e}")



# Example usage
if __name__ == "__main__":
    TOKEN = " "
    verifier = ReceiptVerifier(TOKEN, max_retries=3, cache_size=100)

    # # Example 1: Verify from QR string (most common)
    # qr_string = "t=...&s=...&fn=...&i=...&fp=...&n=1"
    # receipt = from_qr_string(qr_string)


    # Example 2: Verify from manual parameters as JSON
    params = {
        'fn': '...',
        'fd': '...',
        'fp': '...',
        't': '...T...',
        'n': '1',
        's': '1000.00',
        'qr': '0'
    }

    receipt = from_params(params)


    # # Example 3: Verify from QR image URL
    # qr_url = "https://example.com/qr_code.jpg"
    # receipt = from_qr_url(qr_url)


    # # # Example 4: Verify from QR image file
    # file_path = "path/to/qr_image.jpg"
    # receipt = from_qr_img(file_path)


    save_json(receipt)
    print_receipt(receipt)

    # # Display cache statistics
    # print("\n📊 Cache Statistics:")
    # stats = verifier.get_cache_stats()
    # for key, value in stats.items():
    #     print(f"  {key}: {value}")