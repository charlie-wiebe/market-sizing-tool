EMPLOYEE_RANGES = [
    "1-10", "11-20", "21-50", "51-100", "101-200", 
    "201-500", "501-1000", "1001-2000", "2001-5000", 
    "5001-10000", "10000+"
]

COUNTRIES = [
    "United States",
    "United Kingdom", 
    "Canada"
]

MAX_RESULTS_PER_QUERY = 25000

class QuerySegmenter:
    def __init__(self, prospeo_client):
        self.client = prospeo_client

    def estimate_total_count(self, filters):
        response = self.client.search_companies(filters, page=1)
        if self.client.is_error(response):
            return 0, response
        pagination = self.client.get_pagination(response)
        return pagination["total_count"], response

    def needs_segmentation(self, total_count):
        return total_count > MAX_RESULTS_PER_QUERY

    def generate_segments(self, base_filters, total_count):
        if total_count <= MAX_RESULTS_PER_QUERY:
            return [base_filters]
        
        segments = []
        
        has_location = "company_location_search" in base_filters
        has_headcount = "company_headcount_range" in base_filters
        
        if not has_location:
            for country in COUNTRIES:
                segment_filters = dict(base_filters)
                segment_filters["company_location_search"] = {
                    "include": [country],
                    "exclude": []
                }
                segments.append(segment_filters)
        elif not has_headcount:
            for emp_range in EMPLOYEE_RANGES:
                segment_filters = dict(base_filters)
                segment_filters["company_headcount_range"] = [emp_range]
                segments.append(segment_filters)
        else:
            segments.append(base_filters)
        
        return segments

    def create_execution_plan(self, base_filters):
        total_count, initial_response = self.estimate_total_count(base_filters)
        
        if self.client.is_error(initial_response):
            return {
                "error": True,
                "error_code": self.client.get_error_code(initial_response),
                "segments": [],
                "total_estimated": 0,
                "credits_estimate": 0
            }
        
        if not self.needs_segmentation(total_count):
            pages_needed = (total_count + 24) // 25
            return {
                "error": False,
                "segments": [{
                    "filters": base_filters,
                    "estimated_count": total_count,
                    "pages": pages_needed
                }],
                "total_estimated": total_count,
                "credits_estimate": pages_needed
            }
        
        segments = self.generate_segments(base_filters, total_count)
        segment_details = []
        total_pages = 0
        total_estimated = 0
        
        for segment_filters in segments:
            count, _ = self.estimate_total_count(segment_filters)
            pages = (count + 24) // 25
            
            if count > MAX_RESULTS_PER_QUERY:
                sub_segments = self.generate_segments(segment_filters, count)
                for sub_filter in sub_segments:
                    sub_count, _ = self.estimate_total_count(sub_filter)
                    sub_pages = (sub_count + 24) // 25
                    segment_details.append({
                        "filters": sub_filter,
                        "estimated_count": sub_count,
                        "pages": sub_pages
                    })
                    total_pages += sub_pages
                    total_estimated += sub_count
            else:
                segment_details.append({
                    "filters": segment_filters,
                    "estimated_count": count,
                    "pages": pages
                })
                total_pages += pages
                total_estimated += count
        
        return {
            "error": False,
            "segments": segment_details,
            "total_estimated": total_estimated,
            "credits_estimate": total_pages
        }
