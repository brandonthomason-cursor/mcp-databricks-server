"""
Coda API Integration for WBR Updates

This module handles sending partner metrics updates to Coda pages.
Requires Coda API token and document/page IDs.

Usage:
    from coda_updater import CodaUpdater, parse_coda_url
    updater = CodaUpdater(api_token="your_token")
    updater.update_partner_metrics(partner_name, metrics, page_id)
    
    # Or parse URL directly
    doc_id, table_id = parse_coda_url("https://coda.io/d/doc_id/table_name_tableId#pageId")
"""

import os
import re
import requests
from typing import Dict, Optional, Tuple, List
from dataclasses import asdict
from datetime import datetime


def parse_coda_url(url: str) -> Tuple[str, Optional[str]]:
    """
    Parse Coda URL to extract document ID and table ID
    
    Args:
        url: Coda URL (e.g., "https://coda.io/d/doc_id/table_name_tableId#pageId")
    
    Returns:
        Tuple of (document_id, table_id)
    """
    # Pattern: https://coda.io/d/{doc_name}_{doc_id}/{table_name}_{table_id}#{page_id}
    # Note: URL may have 'd' prefix in doc_id (e.g., dG4dJxxWn4e) but API uses without it (G4dJxxWn4e)
    match = re.search(r'/d/[^/]+_d?([A-Za-z0-9]+)/(?:[^#]+_)?([A-Za-z0-9]+)', url)
    if match:
        doc_id = match.group(1)
        table_id = match.group(2) if match.group(2) else None
        return doc_id, table_id
    
    # Fallback: try to extract just doc_id (handle optional 'd' prefix)
    match = re.search(r'/d/[^/]+_d?([A-Za-z0-9]+)', url)
    if match:
        return match.group(1), None
    
    raise ValueError(f"Could not parse Coda URL: {url}")


class CodaUpdater:
    """Handles updates to Coda documents via API"""
    
    def __init__(self, api_token: Optional[str] = None, doc_id: Optional[str] = None, doc_url: Optional[str] = None):
        """
        Initialize Coda updater
        
        Args:
            api_token: Coda API token (or set CODA_API_TOKEN env var)
            doc_id: Coda document ID (or set CODA_DOC_ID env var)
            doc_url: Coda document URL (will parse doc_id from URL if provided)
        """
        self.api_token = api_token or os.environ.get("CODA_API_TOKEN")
        
        # Parse doc_id from URL if provided
        if doc_url and not doc_id:
            doc_id, table_id_from_url = parse_coda_url(doc_url)
            if table_id_from_url:
                self.default_table_id = table_id_from_url
        
        self.doc_id = doc_id or os.environ.get("CODA_DOC_ID")
        self.base_url = "https://coda.io/apis/v1"
        self.default_table_id = getattr(self, 'default_table_id', None)
        
        if not self.api_token:
            raise ValueError("Coda API token required. Set CODA_API_TOKEN env var or pass api_token parameter.")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get API request headers"""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
    
    def list_tables(self, doc_id: Optional[str] = None, include_views: bool = True) -> Dict:
        """List all tables in a Coda document (handles pagination)"""
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        url = f"{self.base_url}/docs/{doc_id}/tables"
        params = {'limit': 100}
        if include_views:
            params['tableTypes'] = 'table,view'
        
        all_items = []
        page_token = None
        
        while True:
            if page_token:
                params['pageToken'] = page_token
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            items = data.get('items', [])
            all_items.extend(items)
            
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        
        return {'items': all_items}
    
    def find_table_by_name(self, table_name: str, doc_id: Optional[str] = None) -> Optional[str]:
        """Find table ID by name"""
        tables = self.list_tables(doc_id)
        for table in tables.get("items", []):
            if table.get("name") == table_name:
                return table.get("id")
        return None
    
    def list_rows(self, table_id: str, doc_id: Optional[str] = None) -> Dict:
        """List rows in a table"""
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        url = f"{self.base_url}/docs/{doc_id}/tables/{table_id}/rows"
        response = requests.get(url, headers=self._get_headers())
        response.raise_for_status()
        return response.json()
    
    def upsert_row(self, table_id: str, row_data: Dict, key_columns: list, doc_id: Optional[str] = None) -> Dict:
        """
        Upsert a row in a table
        
        Args:
            table_id: Table ID
            row_data: Dictionary of column_name: value pairs
            key_columns: List of column names to use as keys for matching
            doc_id: Document ID (optional)
        """
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        url = f"{self.base_url}/docs/{doc_id}/tables/{table_id}/rows"
        
        # Format row data for Coda API
        cells = []
        for col_name, value in row_data.items():
            cells.append({
                "column": col_name,
                "value": value
            })
        
        payload = {
            "rows": [{
                "cells": cells
            }],
            "keyColumns": key_columns
        }
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    
    def delete_rows(self, table_id: str, row_ids: List[str], doc_id: Optional[str] = None) -> bool:
        """
        Delete rows from a table
        
        Args:
            table_id: Table ID
            row_ids: List of row IDs to delete
            doc_id: Document ID (optional)
        
        Returns:
            True if successful, False otherwise
        """
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        if not row_ids:
            return True
        
        url = f"{self.base_url}/docs/{doc_id}/tables/{table_id}/rows"
        
        # Delete rows one by one (Coda API limitation)
        deleted_count = 0
        for row_id in row_ids:
            try:
                delete_url = f"{url}/{row_id}"
                response = requests.delete(delete_url, headers=self._get_headers())
                if response.status_code in [200, 204]:
                    deleted_count += 1
            except:
                pass
        
        return deleted_count == len(row_ids)
    
    def update_partner_metrics(self, partner_name: str, metrics: Dict, table_name: str = "Partner Metrics", table_id: Optional[str] = None, doc_id: Optional[str] = None) -> Dict:
        """
        Update partner metrics in Coda
        
        Args:
            partner_name: Partner name (used as key)
            metrics: Dictionary with metrics:
                - managed_revenue
                - referral_revenue
                - advanced_sales_training
                - advanced_technical_training
                - general_access_training
                - managed_revenue_14d_ago (optional)
                - referral_revenue_14d_ago (optional)
            table_name: Name of the Coda table
            doc_id: Document ID (optional)
        """
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        # Use provided table_id, default_table_id, or find by name
        if not table_id:
            table_id = self.default_table_id
        
        if not table_id:
            table_id = self.find_table_by_name(table_name, doc_id)
            if not table_id:
                raise ValueError(f"Table '{table_name}' not found in document. Available tables: {[t.get('name') for t in self.list_tables(doc_id).get('items', [])]}")
        
        # Prepare row data
        row_data = {
            "Partner": partner_name,
            "Managed Revenue": metrics.get("managed_revenue", 0),
            "Referral Revenue": metrics.get("referral_revenue", 0),
            "Advanced Sales Training": metrics.get("advanced_sales_training", 0),
            "Advanced Technical Training": metrics.get("advanced_technical_training", 0),
            "General Access Training": metrics.get("general_access_training", 0),
            "Last Updated": datetime.now().strftime("%m/%d/%Y")
        }
        
        # Add 14-day comparison if available
        if metrics.get("managed_revenue_14d_ago"):
            row_data["Managed Revenue (14d ago)"] = metrics["managed_revenue_14d_ago"]
            row_data["Managed Revenue Change"] = metrics["managed_revenue"] - metrics["managed_revenue_14d_ago"]
        
        if metrics.get("referral_revenue_14d_ago"):
            row_data["Referral Revenue (14d ago)"] = metrics["referral_revenue_14d_ago"]
            row_data["Referral Revenue Change"] = metrics["referral_revenue"] - metrics["referral_revenue_14d_ago"]
        
        # Upsert row using Partner as key
        return self.upsert_row(table_id, row_data, key_columns=["Partner"], doc_id=doc_id)
    
    def update_partner_status(self, partner_name: str, status_data: Dict, table_name: str = "Partner Status", doc_id: Optional[str] = None) -> Dict:
        """
        Update partner Platinum tier status
        
        Args:
            partner_name: Partner name
            status_data: Dictionary with status info:
                - platinum_status: "Qualified" or "Not Qualified"
                - managed_revenue_gap: Gap to $10,000
                - referral_revenue_gap: Gap to $5,000
                - training_gaps: Dict with gaps for each training type
            table_name: Name of the Coda table
            doc_id: Document ID (optional)
        """
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        table_id = self.find_table_by_name(table_name, doc_id)
        if not table_id:
            raise ValueError(f"Table '{table_name}' not found in document")
        
        row_data = {
            "Partner": partner_name,
            "Platinum Status": status_data.get("platinum_status", "Not Qualified"),
            "Managed Revenue Gap": status_data.get("managed_revenue_gap", 0),
            "Referral Revenue Gap": status_data.get("referral_revenue_gap", 0),
            "Last Updated": datetime.now().strftime("%m/%d/%Y")
        }
        
        return self.upsert_row(table_id, row_data, key_columns=["Partner"], doc_id=doc_id)
    
    def update_accounts_table(self, partner_name: str, accounts: list, table_name: str = None, doc_id: Optional[str] = None) -> Dict:
        """
        Update accounts table in Coda with account metrics
        
        Args:
            partner_name: Partner name (for filtering/grouping)
            accounts: List of AccountSnapshot objects or dictionaries with account data
            table_name: Name of the Coda table (e.g., "Pyxis Accounts" or "{partner_name} Accounts")
            doc_id: Document ID (optional)
        
        Returns:
            Dictionary with update results
        """
        doc_id = doc_id or self.doc_id
        if not doc_id:
            raise ValueError("Document ID required")
        
        # Use partner-specific table name if not provided
        if not table_name:
            table_name = f"{partner_name} Accounts"
        
        table_id = self.find_table_by_name(table_name, doc_id)
        if not table_id:
            raise ValueError(f"Table '{table_name}' not found in document. Available tables: {[t.get('name') for t in self.list_tables(doc_id).get('items', [])]}")
        
        # Convert AccountSnapshot objects to dicts if needed
        account_dicts = []
        for acc in accounts:
            if hasattr(acc, '__dict__'):
                # It's a dataclass/object - try asdict first, fallback to __dict__
                try:
                    acc_dict = asdict(acc)
                except (TypeError, AttributeError):
                    acc_dict = acc.__dict__
            else:
                acc_dict = acc
            
            # Format row data for Coda
            row_data = {
                "Account ID": str(acc_dict.get("account_id", "")),
                "Owner Email": acc_dict.get("owner_email", ""),
                "Company Name": acc_dict.get("company_name", ""),
                "Plan Name": acc_dict.get("plan_name", ""),
                "Plan Family": acc_dict.get("plan_family", ""),
                "Plan ARR (USD)": float(acc_dict.get("plan_arr", 0)),
                "Upmarket Customer": acc_dict.get("upmarket_customer", "No"),
                "Role Vertical": acc_dict.get("role_vertical", ""),
                "Role Clean": acc_dict.get("role_clean", ""),
                "Company Size": acc_dict.get("company_size", ""),
                "Apps Used (28d)": int(acc_dict.get("apps_used_28d", 0)),
                "Tasks Success Billable": int(acc_dict.get("tasks_success_billable", 0)),
                "Last Updated": datetime.now().strftime("%m/%d/%Y"),
                "Partner": partner_name  # For filtering/grouping
            }
            account_dicts.append(row_data)
        
        # Upsert all accounts (using Account ID as key)
        updated_count = 0
        errors = []
        
        for row_data in account_dicts:
            try:
                self.upsert_row(
                    table_id=table_id,
                    row_data=row_data,
                    key_columns=["Account ID"],
                    doc_id=doc_id
                )
                updated_count += 1
            except Exception as e:
                errors.append(f"Account {row_data.get('Account ID')}: {str(e)}")
        
        return {
            "updated": updated_count,
            "total": len(account_dicts),
            "errors": errors
        }


def test_coda_connection(api_token: str, doc_id: str):
    """Test Coda API connection"""
    updater = CodaUpdater(api_token=api_token, doc_id=doc_id)
    tables = updater.list_tables()
    print(f"Connected to Coda document. Found {len(tables.get('items', []))} tables:")
    for table in tables.get("items", [])[:10]:
        print(f"  - {table.get('name')} (ID: {table.get('id')})")
    return tables

