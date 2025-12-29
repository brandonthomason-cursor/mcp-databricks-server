"""
Upmarket Sales Funnel Generator

Categorizes partner-submitted leads by Enterprise/Midmarket/SMB
and calculates funnel metrics with estimated ACV and conversion rates.
"""

from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass


@dataclass
class LeadCategory:
    """Lead categorization by segment"""
    account_id: str
    account_name: str
    partner: str
    segment: str  # Enterprise, Midmarket, SMB
    current_mrr: float
    user_count: int
    referral_arr: float
    lead_score: int
    qualification_tier: str  # Hot, Warm, Cold, Unqualified


@dataclass
class FunnelMetrics:
    """Funnel stage metrics"""
    stage: str
    enterprise_count: int
    midmarket_count: int
    smb_count: int
    total_count: int
    estimated_acv: float  # Average Contract Value
    conversion_rate: float  # Percentage that moves to next stage
    estimated_revenue: float  # Count * ACV * Conversion Rate


class UpmarketFunnelGenerator:
    """Generates upmarket sales funnel from partner leads"""
    
    # Qualification criteria for segment classification
    # These are initial estimates - will be refined with actual data
    ENTERPRISE_CRITERIA = {
        'mrr_threshold': 1000,  # $1,000+ MRR
        'user_threshold': 50,   # 50+ users
        'arr_threshold': 12000,  # $12,000+ ARR
        'lead_score_min': 80    # Hot leads (80+)
    }
    
    MIDMARKET_CRITERIA = {
        'mrr_threshold': 100,   # $100-$999 MRR
        'user_threshold': 10,   # 10-49 users
        'arr_threshold': 1200,  # $1,200-$11,999 ARR
        'lead_score_min': 60    # Warm leads (60-79)
    }
    
    # SMB: Everything below Midmarket thresholds
    
    # Estimated ACV by segment (initial guesses)
    ESTIMATED_ACV = {
        'Enterprise': 50000,   # $50K ACV
        'Midmarket': 15000,    # $15K ACV
        'SMB': 5000            # $5K ACV
    }
    
    # Estimated conversion rates by stage (initial guesses)
    CONVERSION_RATES = {
        'Leads → Qualified': {
            'Enterprise': 0.40,  # 40% of Enterprise leads qualify
            'Midmarket': 0.30,   # 30% of Midmarket leads qualify
            'SMB': 0.20          # 20% of SMB leads qualify
        },
        'Qualified → Opportunity': {
            'Enterprise': 0.60,  # 60% of qualified Enterprise become opportunities
            'Midmarket': 0.50,   # 50% of qualified Midmarket become opportunities
            'SMB': 0.40          # 40% of qualified SMB become opportunities
        },
        'Opportunity → Closed Won': {
            'Enterprise': 0.30,  # 30% of Enterprise opportunities close
            'Midmarket': 0.25,   # 25% of Midmarket opportunities close
            'SMB': 0.20          # 20% of SMB opportunities close
        }
    }
    
    def categorize_lead(self, lead: Dict, partner: str) -> LeadCategory:
        """
        Categorize a lead as Enterprise, Midmarket, or SMB
        
        Args:
            lead: Dictionary with lead data (from lead scoring)
            partner: Partner name
            
        Returns:
            LeadCategory object
        """
        current_mrr = lead.get('current_mrr', 0)
        user_count = lead.get('user_count', 0)
        referral_arr = lead.get('referral_arr', 0)
        lead_score = lead.get('lead_score', 0)
        qualification_tier = lead.get('qualification_tier', 'Unqualified')
        
        # Determine segment based on criteria
        segment = 'SMB'  # Default
        
        # Enterprise criteria (highest priority)
        if (current_mrr >= self.ENTERPRISE_CRITERIA['mrr_threshold'] or
            user_count >= self.ENTERPRISE_CRITERIA['user_threshold'] or
            referral_arr >= self.ENTERPRISE_CRITERIA['arr_threshold'] or
            lead_score >= self.ENTERPRISE_CRITERIA['lead_score_min']):
            segment = 'Enterprise'
        # Midmarket criteria
        elif (current_mrr >= self.MIDMARKET_CRITERIA['mrr_threshold'] or
              user_count >= self.MIDMARKET_CRITERIA['user_threshold'] or
              referral_arr >= self.MIDMARKET_CRITERIA['arr_threshold'] or
              lead_score >= self.MIDMARKET_CRITERIA['lead_score_min']):
            segment = 'Midmarket'
        
        return LeadCategory(
            account_id=str(lead.get('account_id', '')),
            account_name=str(lead.get('account_name', 'Unknown')),
            partner=partner,
            segment=segment,
            current_mrr=float(current_mrr),
            user_count=int(user_count),
            referral_arr=float(referral_arr),
            lead_score=int(lead_score),
            qualification_tier=qualification_tier
        )
    
    def generate_funnel(self, leads_by_partner: Dict[str, List[Dict]], current_quarter_target: float) -> Dict:
        """
        Generate upmarket sales funnel metrics
        
        Args:
            leads_by_partner: Dictionary with partner names as keys and lists of leads as values
            current_quarter_target: Revenue target for current quarter
            
        Returns:
            Dictionary with funnel data ready for Coda table
        """
        # Categorize all leads
        categorized_leads = []
        for partner, leads in leads_by_partner.items():
            for lead in leads:
                categorized = self.categorize_lead(lead, partner)
                categorized_leads.append(categorized)
        
        # Count by segment
        enterprise_count = sum(1 for l in categorized_leads if l.segment == 'Enterprise')
        midmarket_count = sum(1 for l in categorized_leads if l.segment == 'Midmarket')
        smb_count = sum(1 for l in categorized_leads if l.segment == 'SMB')
        total_leads = len(categorized_leads)
        
        # Calculate funnel stages
        # Stage 1: Leads (starting point)
        leads_stage = FunnelMetrics(
            stage='Leads',
            enterprise_count=enterprise_count,
            midmarket_count=midmarket_count,
            smb_count=smb_count,
            total_count=total_leads,
            estimated_acv=0,  # No ACV at lead stage
            conversion_rate=1.0,  # 100% start here
            estimated_revenue=0
        )
        
        # Stage 2: Qualified Leads (use round to avoid truncation to 0)
        qualified_enterprise = round(enterprise_count * self.CONVERSION_RATES['Leads → Qualified']['Enterprise'])
        qualified_midmarket = round(midmarket_count * self.CONVERSION_RATES['Leads → Qualified']['Midmarket'])
        qualified_smb = round(smb_count * self.CONVERSION_RATES['Leads → Qualified']['SMB'])
        qualified_total = qualified_enterprise + qualified_midmarket + qualified_smb
        
        qualified_stage = FunnelMetrics(
            stage='Qualified',
            enterprise_count=qualified_enterprise,
            midmarket_count=qualified_midmarket,
            smb_count=qualified_smb,
            total_count=qualified_total,
            estimated_acv=0,
            conversion_rate=qualified_total / total_leads if total_leads > 0 else 0,
            estimated_revenue=0
        )
        
        # Stage 3: Opportunities
        opp_enterprise = round(qualified_enterprise * self.CONVERSION_RATES['Qualified → Opportunity']['Enterprise'])
        opp_midmarket = round(qualified_midmarket * self.CONVERSION_RATES['Qualified → Opportunity']['Midmarket'])
        opp_smb = round(qualified_smb * self.CONVERSION_RATES['Qualified → Opportunity']['SMB'])
        opp_total = opp_enterprise + opp_midmarket + opp_smb
        
        opportunities_stage = FunnelMetrics(
            stage='Opportunities',
            enterprise_count=opp_enterprise,
            midmarket_count=opp_midmarket,
            smb_count=opp_smb,
            total_count=opp_total,
            estimated_acv=0,
            conversion_rate=opp_total / qualified_total if qualified_total > 0 else 0,
            estimated_revenue=0
        )
        
        # Stage 4: Closed Won (Revenue)
        won_enterprise = round(opp_enterprise * self.CONVERSION_RATES['Opportunity → Closed Won']['Enterprise'])
        won_midmarket = round(opp_midmarket * self.CONVERSION_RATES['Opportunity → Closed Won']['Midmarket'])
        won_smb = round(opp_smb * self.CONVERSION_RATES['Opportunity → Closed Won']['SMB'])
        won_total = won_enterprise + won_midmarket + won_smb
        
        # Calculate estimated revenue
        estimated_revenue = (
            won_enterprise * self.ESTIMATED_ACV['Enterprise'] +
            won_midmarket * self.ESTIMATED_ACV['Midmarket'] +
            won_smb * self.ESTIMATED_ACV['SMB']
        )
        
        closed_won_stage = FunnelMetrics(
            stage='Closed Won',
            enterprise_count=won_enterprise,
            midmarket_count=won_midmarket,
            smb_count=won_smb,
            total_count=won_total,
            estimated_acv=self.ESTIMATED_ACV['Enterprise'],  # Weighted average
            conversion_rate=won_total / opp_total if opp_total > 0 else 0,
            estimated_revenue=estimated_revenue
        )
        
        # Calculate progress toward target
        target_progress = (estimated_revenue / current_quarter_target * 100) if current_quarter_target > 0 else 0
        
        return {
            'funnel_stages': [
                {
                    'Stage': 'Leads',
                    'Enterprise': enterprise_count,
                    'Midmarket': midmarket_count,
                    'SMB': smb_count,
                    'Total': total_leads,
                    'Conversion Rate': '100%',
                    'Estimated Revenue': '$0'
                },
                {
                    'Stage': 'Qualified',
                    'Enterprise': qualified_enterprise,
                    'Midmarket': qualified_midmarket,
                    'SMB': qualified_smb,
                    'Total': qualified_total,
                    'Conversion Rate': f'{qualified_stage.conversion_rate:.1%}',
                    'Estimated Revenue': '$0'
                },
                {
                    'Stage': 'Opportunities',
                    'Enterprise': opp_enterprise,
                    'Midmarket': opp_midmarket,
                    'SMB': opp_smb,
                    'Total': opp_total,
                    'Conversion Rate': f'{opportunities_stage.conversion_rate:.1%}',
                    'Estimated Revenue': '$0'
                },
                {
                    'Stage': 'Closed Won',
                    'Enterprise': won_enterprise,
                    'Midmarket': won_midmarket,
                    'SMB': won_smb,
                    'Total': won_total,
                    'Conversion Rate': f'{closed_won_stage.conversion_rate:.1%}',
                    'Estimated Revenue': f'${estimated_revenue:,.0f}'
                }
            ],
            'summary': {
                'Current Quarter Target': current_quarter_target,
                'Total Leads': total_leads,
                'Enterprise Leads': enterprise_count,
                'Midmarket Leads': midmarket_count,
                'SMB Leads': smb_count,
                'Estimated Revenue': estimated_revenue,
                'Target Progress': f'{target_progress:.1f}%',
                'Gap to Target': current_quarter_target - estimated_revenue
            },
            'categorized_leads': categorized_leads
        }


def get_current_quarter_target() -> float:
    """Get current quarter revenue target
    
    For 2026: Q1 = $20K, Q2 = $80K
    Defaults to Q1 target if current date doesn't match a quarter
    """
    from datetime import datetime
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    
    # For 2026 program: Q1 = $20K, Q2 = $80K
    # If we're in late 2025 or early 2026, default to Q1 2026
    if current_year == 2025 or (current_year == 2026 and current_month <= 3):
        return 20000  # Q1 2026 target
    elif current_year == 2026 and 4 <= current_month <= 6:
        return 80000  # Q2 2026 target
    elif current_year == 2026 and 7 <= current_month <= 9:
        return 0  # Q3 2026 target (not set yet)
    elif current_year == 2026 and 10 <= current_month <= 12:
        return 0  # Q4 2026 target (not set yet)
    else:
        # Default to Q1 2026 target
        return 20000

