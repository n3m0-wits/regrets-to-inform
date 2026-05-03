import pandas as pd
import re

_AUTOFILL_KEYS = {
    'description', 'company', 'school', 'dates of employment', 'dates attended',
    'your title', 'major / field of study', 'degree', 'first name', 'last name',
    'email address', 'mobile phone number', 'please submit a resume or linkedin profile',
    'please submit a resume', 'headline', 'cover letter', 'location (city)',
    'location', 'city', 'state / province', 'country', 'industry', 'photo',
    'summary', 'additional attachments', 'e-mailadres', 'linkedin profile', 'linkedin',
    'what is your current location?', 'mobiel telefoonnummer',
    'dien een cv of linkedin-profiel in',
}

def profile_job_requirements(record):
    job_profile = {}
    
    taxonomy = {
        # Tech & Software
        'Python': ['python'],
        'SQL': ['sql', 'mysql', 'postgresql', 'access', 'nosql'],
        'Excel': ['excel', 'spreadsheet', 'pivot table', 'pivottable'],
        'PowerBI': ['powerbi', 'power bi'],
        'Tableau': ['tableau'],
        'DataVisualisation': ['visualization', 'visualisation'],
        'AWS': ['aws', 'amazon web services'],
        'Azure': ['azure'],
        'Git': ['git', 'version control'],
        'NLP': ['nlp', 'natural language processing'],
        'OCR': ['ocr', 'optical character recognition'],
        'RPA': ['rpa', 'robotic process automation'],
        'GIS': ['gis', 'spatial data'],
        'Pandas_Numpy': ['pandas', 'numpy'],
        'Scikit_Learn': ['scikit-learn', 'scikit learn'],
        'Linux': ['linux', 'linux command line', 'ubuntu'],
        'SSIS': ['ssis', 'sql server integration services'],
        'Medidata': ['medidata', 'medidata rave'],
        'DevOps': ['web development framework', 'backend systems', 'frontend', 'full stack', 'full-stack', 'devops', 'scrum'],
        
        # Domains & Functions
        'DataScience': ['data science', 'machine learning', 'ai ', 'artificial intelligence'],
        'DataEngineering': ['data engineering', 'data pipelines', 'etl', 'extract', 'transform', 'load'],
        'DataAnalysis': ['data analysis', 'analytics', 'business analyst'],
        'BankingFinancial': ['financial services', 'finance', 'investment management', 'portfolio construction', 'banking', 'private client investment platforms'],
        'CustomerSuccess': ['customer success', 'customer support', 'client reporting'],
        'ConsultingStrategy': ['consulting', 'advisory', 'analysis','strategy', 'planning', 'stra', 'digital strategy'],
        'OR':['process', 'operations research', 'process engineer', 'optimisation', 'optimization'],
        'Hardware': ['physical products', 'solar', 'battery', 'inverter', 'networking'],
        'SupplyChain': ['supply chain', 'supl', 'logistics', 'operations'],
        'Sales': ['sales', 'sale', 'b2b'],
        'Management': ['management', 'mgmt'],
        'Engineering': ['engineering', 'eng', 'engineer','construction'],
        'Hospitality': ['hospitality', 'food and beverage services', 'service'],
        'Automotive': ['automotive', 'parts industry'],
        'ClientFacing': ['client-facing', 'customer-facing', 'front office environment'],
        
        # Languages
        'Lang_English': ['english', 'eng'],
        'Lang_Afrikaans': ['afrikaans', 'afr'],
        'Lang_Arabic': ['arabic', 'ara'],
        'Lang_Chinese': ['chinese', 'chi'],
        'Lang_Dutch': ['dutch', 'dut'],
        
        # LinkedIn Industry URN Codes
        'Ind_IT': ['urn:li:industry:116', 'urn:li:industry:96', 'urn:li:industry:4'],
        'Ind_ManagementConsulting': ['urn:li:industry:11'],
        'Ind_RealEstate': ['urn:li:industry:30'],
        'Ind_Logistics': ['urn:li:industry:31'],
        'Ind_Hospitality': ['urn:li:industry:34'],
        'Ind_Banking': ['urn:li:industry:41'],
        'Ind_FinancialServices': ['urn:li:industry:43'],
        'Ind_Construction': ['urn:li:industry:48'],
        'Ind_Healthcare': ['urn:li:industry:14'],
        'Ind_Retail': ['urn:li:industry:27']
    }

    def extract_years(text):
        word_to_num = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}
        pattern = r'(\d+|' + '|'.join(word_to_num.keys()) + r')\s*\+?\s*years?'
        match = re.search(pattern, text)
        if match:
            val = match.group(1)
            return int(val) if val.isdigit() else word_to_num[val]
        return None

    for question in record.keys():
        if not isinstance(question, str):
            continue

        q_lower = question.lower()

        if q_lower in _AUTOFILL_KEYS:
            continue
        
        # Skip Administrative Fluff
        legal_terms = ['certify', 'authorize', 'privacy', 'background check', 'criminal', 'submit a resume', 'cover letter', 'first name', 'last name', 'email', 'phone', 'location (city)']
        if any(term in q_lower for term in legal_terms):
            continue

        req_years = extract_years(q_lower)

        found_topics = []
        for clean_name, synonyms in taxonomy.items():
            if any(re.search(rf'\b{re.escape(syn)}\b', q_lower) for syn in synonyms):
                found_topics.append(clean_name)

        if found_topics:
            for topic in found_topics:
                job_profile[f"Ask_{topic}"] = True
                if req_years is not None:
                    job_profile[f"Req_Years_{topic}"] = req_years
                    
        urn_match = re.search(r'urn:li:industry:(\d+)', q_lower)
        if urn_match:
            already_mapped = any(topic.startswith('Ind_') for topic in found_topics)
            if not already_mapped:
                unknown_code = urn_match.group(1)
                job_profile[f"Ask_Ind_URN_{unknown_code}"] = True
                if req_years is not None:
                    job_profile[f"Req_Years_Ind_URN_{unknown_code}"] = req_years

        if 'hybrid' in q_lower: 
            job_profile['Req_Hybrid'] = True
        elif 'remote' in q_lower: 
            job_profile['Req_Remote'] = True
        elif 'onsite' in q_lower or 'office' in q_lower: 
            job_profile['Req_Onsite'] = True
            
        if 'visa' in q_lower or 'sponsorship' in q_lower or 'legally authorized' in q_lower:
            job_profile['Ask_Visa_Auth'] = True
            
        if 'bachelor' in q_lower or 'bsc' in q_lower: 
            job_profile['Req_Bachelors'] = True
        elif 'master' in q_lower or 'msc' in q_lower: 
            job_profile['Req_Masters'] = True

    return job_profile


def process_job_data(file_paths):
    # Load and concatenate all files
    dfs = []
    for path in file_paths:
        try:
            dfs.append(pd.read_csv(path))
        except FileNotFoundError:
            print(f"Warning: Could not find {path}")
            
    if not dfs:
        return None
        
    df_raw = pd.concat(dfs, ignore_index=True)
    
    split_pattern = re.compile(r'\s*\|\s*(?=[^|:]+:)')
    master_records = []

    # Iterate through each job application
    for index, row in df_raw.iterrows():
        row_data = {
            'Application_Date': row.get('Application Date'),
            'Company_Name': row.get('Company Name'),
            'Job_Title': row.get('Job Title'),
            'Job_Url': row.get('Job Url')
        }
        
        # Parse the Q&A
        qa_string = row.get('Question And Answers')
        parsed_qa = {}
        if pd.notna(qa_string) and isinstance(qa_string, str):
            fields = split_pattern.split(qa_string)
            for field in fields:
                if ':' in field:
                    key, value = field.split(':', 1)
                    parsed_qa[key.strip()] = value.strip()
                    
        # Extract features
        extracted_features = profile_job_requirements(parsed_qa)
        
        # Merge features back with the metadata
        row_data.update(extracted_features)
        master_records.append(row_data)

    df_final = pd.DataFrame(master_records)

    metadata_cols = ['Application_Date', 'Company_Name', 'Job_Title', 'Job_Url']
    df_metadata = df_final[metadata_cols]
    df_features = df_final.drop(columns=metadata_cols)

    df_features = df_features.dropna(axis=1, thresh=2)

    df_final = pd.concat([df_metadata, df_features], axis=1)
    
    fill_values = {col: False for col in df_features.columns}
    df_final = df_final.fillna(fill_values)
    
    return df_final


files_to_process = [
    '../Linkedin_data/Jobs/Job Applications_1.csv', 
    '../Linkedin_data/Jobs/Job Applications.csv'
]

final_dataframe = process_job_data(files_to_process)

if final_dataframe is not None:
    print(final_dataframe.info())
    print(f"\nDataset successfully compiled! Dimensions: {final_dataframe.shape}")
    final_dataframe.to_csv('Linkedin_QuickApply.csv')