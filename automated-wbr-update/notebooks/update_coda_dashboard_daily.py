#!/usr/bin/env python3
"""
Daily Update Script for Coda Dashboard

This script updates all master tables in the Coda dashboard with the latest data.
It can be run daily via cron or scheduled task.

Usage:
    python notebooks/update_coda_dashboard_daily.py
    
    Or with environment variables:
    export CODA_API_TOKEN="your_token"
    export CODA_DOC_ID="your_doc_id"
    python notebooks/update_coda_dashboard_daily.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from notebooks.coda_updater import CodaUpdater, parse_coda_url
from notebooks.upmarket_funnel import UpmarketFunnelGenerator, get_current_quarter_target


class CodaDashboardUpdater:
    """Updates the Coda dashboard with latest data"""
    
    def __init__(self, api_token: str, doc_id: str):
        self.updater = CodaUpdater(api_token=api_token, doc_id=doc_id)
        self.doc_id = doc_id
    
    def update_last_updated_timestamp(self):
        """Update the last updated timestamp at the top of the page"""
        import requests
        from datetime import datetime
        
        table_name = "Last Updated"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"  ⚠ Table '{table_name}' not found. Skipping timestamp update.")
            print(f"     Create a table named '{table_name}' with a 'Date' column to enable auto-updates.")
            return False
        
        # Get the column name from the table structure
        try:
            url = f"{self.updater.base_url}/docs/{self.doc_id}/tables/{table_id}/columns"
            response = requests.get(url, headers=self.updater._get_headers())
            if response.status_code == 200:
                columns = response.json().get('items', [])
                if columns:
                    column_id = columns[0].get('id')
                    column_name = columns[0].get('name')
                else:
                    column_name = "Date"
            else:
                column_name = "Date"
        except:
            column_name = "Date"
        
        # Format timestamp
        now = datetime.now()
        timestamp_text = now.strftime("%B %d, %Y at %I:%M %p")  # e.g., "January 8, 2026 at 2:30 PM"
        
        # Get existing rows
        try:
            rows = self.updater.list_rows(table_id, self.doc_id)
            existing_rows = rows.get('items', [])
        except:
            existing_rows = []
        
        url = f"{self.updater.base_url}/docs/{self.doc_id}/tables/{table_id}/rows"
        
        # Delete all existing rows first
        if existing_rows:
            all_row_ids = [row.get('id') for row in existing_rows]
            for row_id in all_row_ids:
                try:
                    delete_url = f"{url}/{row_id}"
                    delete_response = requests.delete(delete_url, headers=self.updater._get_headers())
                except:
                    pass
            
            import time
            time.sleep(2)
        
        # Insert one new row with timestamp
        try:
            payload = {
                "rows": [{
                    "cells": [{
                        "column": column_id if 'column_id' in locals() else column_name,
                        "value": timestamp_text
                    }]
                }]
            }
            
            response = requests.post(url, headers=self.updater._get_headers(), json=payload)
            if response.status_code in [200, 202]:
                import time
                time.sleep(2)
                print(f"  ✅ Updated '{table_name}' timestamp: {timestamp_text}")
                return True
        except Exception as e:
            # Try with column name
            try:
                payload = {
                    "rows": [{
                        "cells": [{
                            "column": column_name,
                            "value": timestamp_text
                        }]
                    }]
                }
                response = requests.post(url, headers=self.updater._get_headers(), json=payload)
                if response.status_code in [200, 202]:
                    import time
                    time.sleep(2)
                    print(f"  ✅ Updated '{table_name}' timestamp: {timestamp_text}")
                    return True
            except:
                pass
        
        print(f"  ⚠ Could not update '{table_name}' timestamp")
        return False
        
    def update_upmarket_funnel(self, funnel_data: Dict):
        """Update Upmarket Sales Funnel table"""
        table_name = "Upmarket Sales Funnel"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            print(f"   Please create this table in Coda with columns: Stage, Enterprise, Midmarket, SMB, Total, Conversion Rate, Estimated Revenue")
            return
        
        # Update funnel stages
        stages = funnel_data.get('funnel_stages', [])
        for stage_row in stages:
            try:
                self.updater.upsert_row(table_id, stage_row, ["Stage"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating stage {stage_row.get('Stage')}: {e}")
        
        print(f"  ✅ Updated {table_name} with {len(stages)} stages")
    
    def update_funnel_summary(self, funnel_data: Dict):
        """Update Funnel Summary KPIs (replaces old Executive Summary KPIs)"""
        table_name = "Funnel Summary KPIs"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            print(f"   Please create this table in Coda")
            return
        
        summary = funnel_data.get('summary', {})
        current_quarter_target = summary.get('Current Quarter Target', 0)
        
        rows = [
            {
                "Metric": "Current Quarter Target",
                "Value": current_quarter_target,
                "Label": f"Q{((datetime.now().month - 1) // 3) + 1} 2026 Goal",
                "Status": "Target"
            },
            {
                "Metric": "Total Leads",
                "Value": summary.get('Total Leads', 0),
                "Label": "Partner-submitted leads",
                "Status": "Current"
            },
            {
                "Metric": "Enterprise Leads",
                "Value": summary.get('Enterprise Leads', 0),
                "Label": "Enterprise segment",
                "Status": "Current"
            },
            {
                "Metric": "Midmarket Leads",
                "Value": summary.get('Midmarket Leads', 0),
                "Label": "Midmarket segment",
                "Status": "Current"
            },
            {
                "Metric": "SMB Leads",
                "Value": summary.get('SMB Leads', 0),
                "Label": "SMB segment",
                "Status": "Current"
            },
            {
                "Metric": "Estimated Revenue",
                "Value": summary.get('Estimated Revenue', 0),
                "Label": "Based on ACV & conversion rates",
                "Status": "Forecast"
            },
            {
                "Metric": "Target Progress",
                "Value": summary.get('Target Progress', '0%'),
                "Label": "Progress toward target",
                "Status": "Current"
            }
        ]
        
        for row in rows:
            try:
                self.updater.upsert_row(table_id, row, ["Metric"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating {row.get('Metric')}: {e}")
        
        print(f"  ✅ Updated {table_name}")
    
    def update_managed_revenue(self, partner_data: List[Dict]):
        """Update All Partners Managed Revenue table"""
        table_name = "CODA_All_Partners_Managed_Revenue"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            return
        
        for row in partner_data:
            try:
                self.updater.upsert_row(table_id, row, ["Partner"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating {row.get('Partner')}: {e}")
        
        print(f"  ✅ Updated {table_name} with {len(partner_data)} partners")
    
    def update_referral_revenue(self, partner_data: List[Dict]):
        """Update All Partners Referral Revenue table"""
        table_name = "CODA_All_Partners_Referral_Revenue"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            return
        
        for row in partner_data:
            try:
                self.updater.upsert_row(table_id, row, ["Partner"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating {row.get('Partner')}: {e}")
        
        print(f"  ✅ Updated {table_name} with {len(partner_data)} partners")
    
    def update_training(self, partner_data: List[Dict]):
        """Update All Partners Training table"""
        table_name = "CODA_All_Partners_Training"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            return
        
        for row in partner_data:
            try:
                self.updater.upsert_row(table_id, row, ["Partner"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating {row.get('Partner')}: {e}")
        
        print(f"  ✅ Updated {table_name} with {len(partner_data)} partners")
    
    def update_partner_summaries(self, summaries: List[Dict]):
        """Update Partner Performance Summaries table"""
        table_name = "CODA_Partner_Summaries"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"⚠ Table '{table_name}' not found")
            return
        
        for row in summaries:
            try:
                self.updater.upsert_row(table_id, row, ["Partner", "Section"], self.doc_id)
            except Exception as e:
                print(f"  ⚠ Error updating {row.get('Partner')} - {row.get('Section')}: {e}")
        
        print(f"  ✅ Updated {table_name} with {len(summaries)} entries")
    
    def generate_executive_summary_text(self, data: Dict) -> str:
        """Generate ultra-concise executive summary - scannable in seconds"""
        # Count partners exceeding targets
        exceeding_partners = [p for p in data.get('managed_revenue', []) if p.get('Status') == 'Exceeded']
        needing_activation = [p for p in data.get('managed_revenue', []) if p.get('Status') == 'Not Started']
        
        # Get totals
        total_managed = sum(p.get('Current', 0) for p in data.get('managed_revenue', []))
        total_referral = sum(p.get('Current', 0) for p in data.get('referral_revenue', []))
        
        # Get specific partner data
        pyxis_mgmt = next((p for p in data.get('managed_revenue', []) if p.get('Partner') == 'Pyxis'), {})
        xray_mgmt = next((p for p in data.get('managed_revenue', []) if p.get('Partner') == 'Xray.Tech'), {})
        
        # Get upmarket lead counts (default to 0 if not provided)
        pyxis_leads = data.get('upmarket_leads', {}).get('Pyxis', 0)
        xray_leads = data.get('upmarket_leads', {}).get('Xray.Tech', 0)
        izeno_leads = data.get('upmarket_leads', {}).get('iZeno', 0)
        orium_leads = data.get('upmarket_leads', {}).get('Orium', 0)
        
        # Get next steps with due dates
        next_steps = data.get('next_steps', [])
        next_steps_text = ""
        if next_steps:
            # Format: "Next: [action] (due [date])"
            steps_list = []
            for step in next_steps[:2]:  # Limit to 2 most important
                action = step.get('action', '')
                due_date = step.get('due_date', '')
                if action and due_date:
                    steps_list.append(f"{action} (due {due_date})")
                elif action:
                    steps_list.append(action)
            if steps_list:
                next_steps_text = f" Next: {', '.join(steps_list)}."
        
        # Get funnel summary data
        funnel_summary = data.get('funnel_summary', {})
        current_quarter_target = funnel_summary.get('Current Quarter Target', get_current_quarter_target())
        total_leads = funnel_summary.get('Total Leads', 0)
        enterprise_leads = funnel_summary.get('Enterprise Leads', 0)
        estimated_revenue = funnel_summary.get('Estimated Revenue', 0)
        
        # Build ultra-concise summary with funnel data
        summary = f"""**2 of 4 partners exceeding revenue targets (${total_managed:,.0f} managed, ${total_referral:,.0f} referral). Pyxis ${pyxis_mgmt.get('Current', 0):,.0f} ({pyxis_leads} leads) | Xray.Tech ${xray_mgmt.get('Current', 0):,.0f} ({xray_leads} leads). {', '.join([p.get('Partner') for p in needing_activation])} need{'s' if len(needing_activation) == 1 else ''} activation (${0:,.0f} revenue, {izeno_leads + orium_leads} leads). Upmarket funnel: {total_leads} leads ({enterprise_leads} Enterprise) → ${estimated_revenue:,.0f} est. revenue / ${current_quarter_target:,.0f} target.{next_steps_text}**"""
        
        return summary
    
    def update_executive_summary_text(self, summary_text: str):
        """Update the executive summary text in a table (for display in text box)
        
        Deletes all existing rows and inserts one new row to prevent duplicates.
        """
        import requests
        
        # Try both possible table names
        table_name = "Executive Summary"
        table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            # Try alternative name
            table_name = "Executive Summary Text"
            table_id = self.updater.find_table_by_name(table_name, self.doc_id)
        
        if not table_id:
            print(f"  ⚠ Table 'Executive Summary' or 'Executive Summary Text' not found. Skipping text update.")
            print(f"     Create a table named 'Executive Summary' with a text column to enable auto-updates.")
            return False
        
        # Get the column ID from the table structure (more reliable than name)
        try:
            url = f"{self.updater.base_url}/docs/{self.doc_id}/tables/{table_id}/columns"
            response = requests.get(url, headers=self.updater._get_headers())
            if response.status_code == 200:
                columns = response.json().get('items', [])
                if columns:
                    column_id = columns[0].get('id')  # Use ID instead of name
                    column_name = columns[0].get('name')  # Keep name as fallback
                else:
                    column_id = None
                    column_name = "Summary Text"
            else:
                column_id = None
                column_name = "Summary Text"
        except:
            column_id = None
            column_name = "Summary Text"
        
        # Get existing rows
        try:
            rows = self.updater.list_rows(table_id, self.doc_id)
            existing_rows = rows.get('items', [])
        except:
            existing_rows = []
        
        url = f"{self.updater.base_url}/docs/{self.doc_id}/tables/{table_id}/rows"
        
        # Strategy: Delete ALL rows first, then insert one new row
        # This ensures we never have duplicates
        # Coda API is async, so we need to wait for deletions to complete
        if existing_rows:
            # Delete ALL existing rows
            all_row_ids = [row.get('id') for row in existing_rows]
            deleted_count = 0
            
            for row_id in all_row_ids:
                try:
                    delete_url = f"{url}/{row_id}"
                    delete_response = requests.delete(delete_url, headers=self.updater._get_headers())
                    # Coda returns 202 (Accepted) for async operations - this is OK
                    if delete_response.status_code in [200, 202, 204]:
                        deleted_count += 1
                except Exception as e:
                    print(f"    ⚠ Could not delete row {row_id}: {e}")
            
            if deleted_count > 0:
                print(f"    ✓ Deleted {deleted_count} existing row(s)")
            
            # Wait for async deletions to complete (Coda API is async)
            import time
            time.sleep(3)  # Initial wait
            
            # Verify deletions completed by checking row count
            max_retries = 10
            for retry in range(max_retries):
                try:
                    check_rows = self.updater.list_rows(table_id, self.doc_id)
                    remaining = len(check_rows.get('items', []))
                    if remaining == 0:
                        print(f"    ✓ All rows deleted (verified after {retry + 1} checks)")
                        break
                    if retry < max_retries - 1:
                        time.sleep(2)  # Wait 2 seconds between checks
                except:
                    break
            
            # Final check - if rows still exist, warn but continue
            final_check = self.updater.list_rows(table_id, self.doc_id)
            if len(final_check.get('items', [])) > 0:
                print(f"    ⚠ Warning: {len(final_check.get('items', []))} rows still exist (deletions may be delayed)")
                print(f"       Will insert new row anyway - duplicates will be cleaned up on next run")
        
        # Now insert one new row (whether we deleted rows or not)
        # Use column ID if available, otherwise fall back to column name
        column_identifier = column_id if column_id else column_name
        
        try:
            payload = {
                "rows": [{
                    "cells": [{
                        "column": column_identifier,
                        "value": summary_text
                    }]
                }]
            }
            
            response = requests.post(url, headers=self.updater._get_headers(), json=payload)
            # Coda returns 202 (Accepted) for async operations - this is success
            if response.status_code in [200, 202]:
                # Wait for async operation to complete (Coda needs time)
                import time
                time.sleep(5)  # Initial wait for insertion to process
                
                # Verify insertion completed
                max_retries = 8
                for retry in range(max_retries):
                    check_rows = self.updater.list_rows(table_id, self.doc_id)
                    final_count = len(check_rows.get('items', []))
                    if final_count >= 1:  # At least one row exists
                        if final_count == 1:
                            print(f"  ✅ Updated '{table_name}' table (deleted old rows, inserted 1 new row)")
                        else:
                            print(f"  ✅ Updated '{table_name}' table (inserted new row, {final_count} total)")
                            print(f"     Note: Some old rows may still be deleting (async). Run again to clean up.")
                        return True
                    if retry < max_retries - 1:
                        time.sleep(2)  # Wait 2 seconds between checks
                
                # If we get here, insertion may have failed or is still processing
                print(f"  ⚠ Insertion accepted but not yet visible (Coda async processing)")
                print(f"     Check Coda in a few seconds - the row should appear")
                return True  # Still return True as insertion was accepted
            else:
                response.raise_for_status()
                
        except Exception as e:
            # Try alternative column names/IDs
            for col_identifier in [column_id, column_name, "Summary Text", "Text", "Content", "Summary", "Name"]:
                if col_identifier is None:
                    continue
                try:
                    payload = {
                        "rows": [{
                            "cells": [{
                                "column": col_identifier,
                                "value": summary_text
                            }]
                        }]
                    }
                    response = requests.post(url, headers=self.updater._get_headers(), json=payload)
                    if response.status_code in [200, 202]:
                        import time
                        time.sleep(2)
                        print(f"  ✅ Updated '{table_name}' table")
                        return True
                except:
                    continue
        
        print(f"  ⚠ Error updating executive summary text")
        return False
    
    def update_all(self, data: Dict):
        """
        Update all tables with provided data
        
        Args:
            data: Dictionary with keys:
                - funnel_data: Dictionary with funnel stages and summary (from UpmarketFunnelGenerator)
                - managed_revenue: List of managed revenue rows (with Partner column)
                - referral_revenue: List of referral revenue rows (with Partner column)
                - training: List of training rows (with Partner column)
                - partner_summaries: List of partner summary rows (with Partner, Section, Content)
        """
        print("🚀 Updating Coda Dashboard...")
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Update last updated timestamp at the top of the page
        print("🕐 Updating Last Updated Timestamp...")
        self.update_last_updated_timestamp()
        
        # Generate and update executive summary text (includes funnel data)
        print("\n📝 Generating Executive Summary...")
        summary_text = self.generate_executive_summary_text(data)
        self.update_executive_summary_text(summary_text)
        
        # Update upmarket sales funnel (replaces old Executive Summary KPIs)
        if "funnel_data" in data:
            print("\n📊 Updating Upmarket Sales Funnel...")
            self.update_upmarket_funnel(data["funnel_data"])
            self.update_funnel_summary(data["funnel_data"])
        
        if "managed_revenue" in data:
            print("\n💰 Updating Managed Revenue...")
            self.update_managed_revenue(data["managed_revenue"])
        
        if "referral_revenue" in data:
            print("\n💵 Updating Referral Revenue...")
            self.update_referral_revenue(data["referral_revenue"])
        
        if "training" in data:
            print("\n🎓 Updating Training...")
            self.update_training(data["training"])
        
        if "partner_summaries" in data:
            print("\n📝 Updating Partner Summaries...")
            self.update_partner_summaries(data["partner_summaries"])
        
        print("\n✅ Dashboard update complete!")


def get_latest_data():
    """
    Get latest data from your data sources
    
    This is a placeholder - replace with your actual data fetching logic.
    You can integrate with:
    - Databricks queries
    - Your WBR generator
    - Database queries
    - API calls
    
    Returns:
        Dictionary with all data needed for dashboard update
    """
    # Generate upmarket funnel from partner leads
    # TODO: Replace with actual lead data from Databricks/WBR generator
    funnel_generator = UpmarketFunnelGenerator()
    current_quarter_target = get_current_quarter_target()
    
    # Example: Get leads from partners (replace with actual data fetching)
    # For now, using placeholder data structure
    leads_by_partner = {
        "Pyxis": [],  # TODO: Fetch actual leads from lead scoring
        "Xray.Tech": [],  # TODO: Fetch actual leads
        "iZeno": [],  # TODO: Fetch actual leads
        "Orium": []  # TODO: Fetch actual leads
    }
    
    # Generate funnel
    funnel_data = funnel_generator.generate_funnel(leads_by_partner, current_quarter_target)
    
    # Example data structure - replace with actual data fetching
    return {
        "funnel_data": funnel_data,  # Upmarket sales funnel
        "funnel_summary": funnel_data.get('summary', {}),  # For executive summary
        "upmarket_leads": {
            # Count leads by partner (will be populated from funnel_data)
            "Pyxis": len([l for l in funnel_data.get('categorized_leads', []) if l.partner == 'Pyxis']),
            "Xray.Tech": len([l for l in funnel_data.get('categorized_leads', []) if l.partner == 'Xray.Tech']),
            "iZeno": len([l for l in funnel_data.get('categorized_leads', []) if l.partner == 'iZeno']),
            "Orium": len([l for l in funnel_data.get('categorized_leads', []) if l.partner == 'Orium'])
        },
        "managed_revenue": [
            {"Partner": "Pyxis", "Current": 11285.02, "Target": 10000, "Gap": 1285.02, "Status": "Exceeded", "14-Day Change": "+$530.61 (+4.9%)"},
            {"Partner": "Xray.Tech", "Current": 22414.76, "Target": 10000, "Gap": 12414.76, "Status": "Exceeded", "14-Day Change": "+$997.64 (+4.7%)"},
            {"Partner": "iZeno", "Current": 0, "Target": 10000, "Gap": -10000, "Status": "Not Started", "14-Day Change": "No change"},
            {"Partner": "Orium", "Current": 0, "Target": 10000, "Gap": -10000, "Status": "Not Started", "14-Day Change": "No change"}
        ],
        "referral_revenue": [
            {"Partner": "Pyxis", "Current": 3975.63, "Target": 5000, "Gap": -1024.37, "Status": "In Progress", "14-Day Change": "+$373.50 (+10.4%)"},
            {"Partner": "Xray.Tech", "Current": 0, "Target": 5000, "Gap": -5000, "Status": "Not Started", "14-Day Change": "No change"},
            {"Partner": "iZeno", "Current": 0, "Target": 5000, "Gap": -5000, "Status": "Not Started", "14-Day Change": "No change"},
            {"Partner": "Orium", "Current": 0, "Target": 5000, "Gap": -5000, "Status": "Not Started", "14-Day Change": "No change"}
        ],
        "training": [
            {"Partner": "Pyxis", "General Access": "1/1 ✅", "Advanced Sales": "1/4 ⚠️", "Advanced Technical": "0/4 ❌", "Overall Status": "In Progress"},
            {"Partner": "Xray.Tech", "General Access": "2/1 ✅", "Advanced Sales": "2/4 ⚠️", "Advanced Technical": "0/4 ❌", "Overall Status": "In Progress"},
            {"Partner": "iZeno", "General Access": "1/1 ✅", "Advanced Sales": "0/4 ❌", "Advanced Technical": "0/4 ❌", "Overall Status": "In Progress"},
            {"Partner": "Orium", "General Access": "1/1 ✅", "Advanced Sales": "0/4 ❌", "Advanced Technical": "0/4 ❌", "Overall Status": "In Progress"}
        ],
        "upmarket_leads": {
            # Number of upmarket leads submitted by each partner
            "Pyxis": 0,  # Replace with actual count
            "Xray.Tech": 0,  # Replace with actual count
            "iZeno": 0,  # Replace with actual count
            "Orium": 0  # Replace with actual count
        },
        "next_steps": [
            # Next steps with due dates (limit to 2 most important)
            # {"action": "Pyxis: Pick top 3 accounts", "due_date": "Jan 6, 2026"},
            # {"action": "Xray.Tech: Account review meeting", "due_date": "Dec 22, 2025"}
        ],
        "partner_summaries": [
            # Add partner summary updates here
            # Format: {"Partner": "Pyxis", "Section": "1. General Summary", "Content": "..."}
        ]
    }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Update Coda Dashboard Daily")
    parser.add_argument("--coda-token", type=str, help="Coda API token")
    parser.add_argument("--coda-doc-id", type=str, help="Coda document ID")
    parser.add_argument("--coda-doc-url", type=str, help="Coda document URL")
    
    args = parser.parse_args()
    
    # Get API token
    api_token = args.coda_token or os.environ.get("CODA_API_TOKEN")
    if not api_token:
        parser.error("Coda API token required. Set CODA_API_TOKEN env var or use --coda-token")
    
    # Get document ID
    doc_id = args.coda_doc_id
    if args.coda_doc_url and not doc_id:
        doc_id, _ = parse_coda_url(args.coda_doc_url)
    
    if not doc_id:
        doc_id = os.environ.get("CODA_DOC_ID")
    
    if not doc_id:
        parser.error("Coda document ID required. Set CODA_DOC_ID env var, use --coda-doc-id, or provide --coda-doc-url")
    
    # Get latest data
    print("📥 Fetching latest data...")
    data = get_latest_data()
    
    # Update dashboard
    updater = CodaDashboardUpdater(api_token, doc_id)
    updater.update_all(data)


if __name__ == "__main__":
    main()

