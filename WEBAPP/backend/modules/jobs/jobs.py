from flask import Blueprint, request, jsonify
from modules.helper_functions import get_job_list,call_create_update_job,get_mapping_ref,get_mapping_details
from database.dbconnect import create_oracle_connection
import os
import dotenv
dotenv.load_dotenv()
ORACLE_SCHEMA = os.getenv("SCHEMA")
# Create blueprint
jobs_bp = Blueprint('jobs', __name__)



@jobs_bp.route("/jobs_list", methods=["GET"])
def jobs():
    try:
        conn = create_oracle_connection()
        try:
            job_list = get_job_list(conn)
           
            # Convert datetime objects to ISO format strings for JSON serialization
            for job in job_list:
                if 'RECCRDT' in job and job['RECCRDT']:
                    job['RECCRDT'] = job['RECCRDT'].isoformat()
                if 'RECUPDT' in job and job['RECUPDT']:
                    job['RECUPDT'] = job['RECUPDT'].isoformat()
           
            return jsonify(job_list)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@jobs_bp.route("/view_mapping/<mapping_reference>")
def job_mapping_view(mapping_reference):
    try:
        conn = create_oracle_connection()
        try:
            # Get mapping reference and details data
            mapping_ref_data = get_mapping_ref(conn, reference=mapping_reference)
            mapping_detail_data = get_mapping_details(conn, reference=mapping_reference)
           
            # Prepare the response
            response_data = {
                "mapping_reference": mapping_ref_data,
                "mapping_details": mapping_detail_data
            }
           
            return jsonify(response_data)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@jobs_bp.route('/create-update', methods=['POST'])
def create_update_job():
    try:
        data = request.json
        p_mapref = data.get('mapref')
        
        if not p_mapref:
            return jsonify({
                'success': False,
                'message': 'Missing required parameter: mapref'
            }), 400
            
        conn = create_oracle_connection()
        try:
            job_id, error_message = call_create_update_job(conn, p_mapref)
            
            if error_message:
                return jsonify({
                    'success': False,
                    'message': error_message
                }), 500
                
            return jsonify({
                'success': True,
                'message': 'Job created/updated successfully',
                'job_id': job_id
            })
            
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error in create_update_job: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'An error occurred while processing the request: {str(e)}'
        }), 500 

@jobs_bp.route('/get_all_jobs', methods=['GET'])
def get_all_jobs():
    try:
        conn = create_oracle_connection()
        query_job_flow = f"""

               SELECT 
            f.JOBFLWID,
            f.MAPREF,
            f.TRGSCHM,
            f.TRGTBTYP,
            f.TRGTBNM,
            f.DWLOGIC,
            f.STFLG,
            CASE 
                WHEN s.SCHFLG = 'Y' THEN 'Scheduled'
                ELSE 'Not Scheduled'
            END AS JOB_SCHEDULE_STATUS,
            s.JOBSCHID,
            s.DPND_JOBSCHID,
            s.FRQCD AS "Frequency code",
            s.FRQDD AS "Frequency day",
            s.FRQHH AS "frequency hour",
            s.FRQMI AS "frequency month",
            s.STRTDT AS "start date",
            s.ENDDT AS "end date"
        FROM 
            {ORACLE_SCHEMA}.DWJOBFLW f
        LEFT JOIN 
            (
                SELECT 
                    JOBFLWID, 
                    MIN(JOBSCHID) AS JOBSCHID, 
                    MIN(DPND_JOBSCHID) AS DPND_JOBSCHID,
                    MIN(FRQCD) AS FRQCD,
                    MIN(FRQDD) AS FRQDD,
                    MIN(FRQHH) AS FRQHH,
                    MIN(FRQMI) AS FRQMI,
                    MIN(STRTDT) AS STRTDT,
                    MIN(ENDDT) AS ENDDT,
                    MAX(SCHFLG) AS SCHFLG
                FROM 
                    {ORACLE_SCHEMA}.DWJOBSCH
                WHERE 
                    CURFLG = 'Y'
                GROUP BY 
                    JOBFLWID
            ) s
        ON 
            f.JOBFLWID = s.JOBFLWID
        WHERE 
            f.CURFLG = 'Y'


        """
        cursor = conn.cursor()
        cursor.execute(query_job_flow)
        columns = [col[0] for col in cursor.description]
        raw_jobs = cursor.fetchall()
    
        # Convert LOB objects to strings and create a list of dictionaries
        jobs = []
        for row in raw_jobs:
            job_dict = {}
            for i, column in enumerate(columns):
                value = row[i]
                # Handle LOB objects
                if hasattr(value, 'read'):
                    try:
                        value = value.read()
                        # If it's bytes, decode to string
                        if isinstance(value, bytes):
                            value = value.decode('utf-8')
                    except Exception as e:
                        value = str(e)  # Fallback if reading fails
                job_dict[column] = value
            jobs.append(job_dict)
            
        return jsonify(jobs)
    except Exception as e:
        print(f"Error in get_all_jobs: {str(e)}")
        return jsonify({"error": str(e)}), 500

# get job details
@jobs_bp.route('/get_job_details/<mapref>', methods=['GET'])
def get_job_details(mapref):
    try:
        conn = create_oracle_connection()

        job_details_query=""" 
        SELECT TRGCLNM,TRGCLDTYP,TRGKEYFLG,TRGKEYSEQ,TRGCLDESC,MAPLOGIC,KEYCLNM,VALCLNM,SCDTYP FROM DWJOBDTL WHERE CURFLG = 'Y' AND MAPREF = :mapref """
        cursor = conn.cursor()
        cursor.execute(job_details_query, {'mapref': mapref})
        job_details = cursor.fetchall()
        return jsonify({
            'job_details': job_details
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# get job schedule details
@jobs_bp.route('/get_job_schedule_details/<job_flow_id>', methods=['GET'])
def get_job_schedule_details(job_flow_id):
    try:
        conn = create_oracle_connection()
        query = """
        SELECT 
            JOBFLWID,
            MAPREF,
            FRQCD,
            FRQDD,
            FRQHH,
            FRQMI,
            STRTDT,
            ENDDT,
            STFLG,
            DPND_JOBSCHID,
            RECCRDT,
            RECUPDT 
        FROM DWJOBSCH 
        WHERE CURFLG ='Y' AND JOBFLWID=:job_flow_id
        """
        cursor = conn.cursor()
        cursor.execute(query, {'job_flow_id': job_flow_id})
        
        # Get column names
        columns = [col[0] for col in cursor.description]
        
        # Convert to list of dictionaries
        job_schedule_details = []
        for row in cursor.fetchall():
            job_dict = {}
            for i, column in enumerate(columns):
                value = row[i]
                
                # Convert Oracle NUMBER to string or int
                if isinstance(value, int) or isinstance(value, float):
                    job_dict[column] = str(value)  # Convert all numbers to strings for consistency
                # Handle date objects
                elif hasattr(value, 'strftime'):
                    job_dict[column] = value.isoformat() if hasattr(value, 'isoformat') else str(value)
                # Handle other types
                else:
                    job_dict[column] = str(value) if value is not None else ""
            
            # Debug information
            print(f"Job schedule details for {job_flow_id}: {job_dict}")
            job_schedule_details.append(job_dict)
        
        return jsonify(job_schedule_details)
    except Exception as e:
        print(f"Error in get_job_schedule_details: {str(e)}")
        return jsonify({"error": str(e)}), 500



####################### Scheduled Jobs and Logs #######################

# get list of scheduled jobs
@jobs_bp.route('/get_scheduled_jobs', methods=['GET'])
def get_scheduled_jobs():
    try:
        conn = create_oracle_connection()
        query = """ 
        SELECT 
            MAPREF as MAP_REFERENCE,
            STFLG as STATUS,
            FRQCD as FREQUENCY_CODE,
            FRQDD as FREQUENCY_DAY,
            FRQHH as FREQUENCY_HOUR,
            FRQMI as FREQUENCY_MINUTE
        FROM DWJOBSCH 
        WHERE CURFLG = 'Y' 
        """
        cursor = conn.cursor()
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        scheduled_jobs = cursor.fetchall()
        
        # Debug information
        print(f"Column names: {column_names}")
        print(f"Number of jobs found: {len(scheduled_jobs)}")
        if len(scheduled_jobs) > 0:
            print(f"Sample job data: {scheduled_jobs[0]}")
            
        return jsonify({
            'scheduled_jobs': scheduled_jobs
        })
    except Exception as e:
        print(f"Error in get_scheduled_jobs: {str(e)}")
        return jsonify({"error": str(e)}), 500


# get job and process log details for a scheduled job
@jobs_bp.route('/get_job_and_process_log_details/<mapref>', methods=['GET'])
def get_job_and_process_log_details(mapref):
    try:
        conn = create_oracle_connection()
        query = """ 
        SELECT 
            DJL.PRCDT as PROCESS_DATE,
            DJL.MAPREF as MAP_REFERENCE,
            DJL.JOBID as JOB_ID,
            DJL.SRCROWS as SOURCE_ROWS,
            DJL.TRGROWS as TARGET_ROWS,
            DJL.ERRROWS as ERROR_ROWS,
            DPL.STRTDT as START_DATE,
            DPL.ENDDT as END_DATE,
            DPL.STATUS as STATUS
        FROM 
            TRG.DWJOBLOG DJL
        INNER JOIN 
            TRG.DWPRCLOG DPL
        ON 
            DJL.JOBID = DPL.JOBID
        WHERE 
            DJL.MAPREF = :mapref
        """
        cursor = conn.cursor()
        cursor.execute(query, {'mapref': mapref})
        job_and_process_log_details = cursor.fetchall()
        return jsonify({
            'job_and_process_log_details': job_and_process_log_details
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get error details of a scheduled job
@jobs_bp.route('/get_error_details/<job_id>', methods=['GET'])
def get_error_details(job_id):
    try:
        conn = create_oracle_connection()
        query = """ 
        SELECT ERRID as ERROR_ID,
        PRCDT as PROCESS_DATE,
        ERRTYP as ERROR_TYPE,
        DBERRMSG as DATABASE_ERROR_MESSAGE,
        ERRMSG as ERROR_MESSAGE,
        KEYVALUE as KEY_VALUE 
        FROM DWJOBERR WHERE JOBID = :job_id 
        """
        cursor = conn.cursor()
        cursor.execute(query, {'job_id': job_id})
        error_details = cursor.fetchall()
        return jsonify({
            'error_details': error_details
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500




# save or update job schedule
@jobs_bp.route('/save_job_schedule', methods=['POST'])
def save_job_schedule():
    try:
        data = request.json
        
        # Required fields
        job_flow_id = data.get('JOBFLWID')
        map_ref = data.get('MAPREF')
        frequency_code = data.get('FRQCD')
        frequency_day = data.get('FRQDD')
        frequency_hour = data.get('FRQHH')
        frequency_minute = data.get('FRQMI')
        start_date = data.get('STRTDT')
        end_date = data.get('ENDDT')
        
        conn = create_oracle_connection()
        try:
            cursor = conn.cursor()
            # Call the Oracle package function
            sql = f"""
            DECLARE
                v_jobschid NUMBER;
            BEGIN
                v_jobschid := {ORACLE_SCHEMA}.PKGDWPRC.CREATE_JOB_SCHEDULE(
                    p_mapref => :p_mapref,
                    p_frqcd => :p_frqcd,
                    p_frqdd => :p_frqdd,
                    p_frqhh => :p_frqhh,
                    p_frqmi => :p_frqmi,
                    p_strtdt => TO_DATE(:p_strtdt, 'YYYY-MM-DD'),
                    p_enddt => TO_DATE(:p_enddt, 'YYYY-MM-DD')
                );
                :job_schedule_id := v_jobschid;
            END;
            """
            
            # Prepare the parameters
            job_schedule_id = cursor.var(int)
            
            # Handle end_date, which can be null
            end_date_param = end_date if end_date else None
            
            # Execute the PL/SQL block
            cursor.execute(sql, {
                'p_mapref': map_ref,
                'p_frqcd': frequency_code,
                'p_frqdd': frequency_day,
                'p_frqhh': frequency_hour,
                'p_frqmi': frequency_minute,
                'p_strtdt': start_date,
                'p_enddt': end_date_param,
                'job_schedule_id': job_schedule_id
            })
            
            # Commit the transaction
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': 'Job schedule saved successfully',
                'job_schedule_id': job_schedule_id.getvalue()
            })
            
        except Exception as e:
            conn.rollback()
            error_message = str(e)
            print(f"Database error in save_job_schedule: {error_message}")
            return jsonify({
                'success': False,
                'message': f'Database error: {error_message}'
            }), 500
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error in save_job_schedule: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'An error occurred while saving the job schedule: {str(e)}'
        }), 500



# save save parent and child job.
@jobs_bp.route('/save_parent_child_job', methods=['POST'])
def save_parent_child_job():
    try:
        data = request.json
        
        # Required fields
        parent_map_reference = data.get('PARENT_MAP_REFERENCE')
        child_map_reference = data.get('CHILD_MAP_REFERENCE')
        
        # Validate required fields
        if not parent_map_reference or not child_map_reference:
            return jsonify({
                'success': False,
                'message': 'Missing required parameters: PARENT_MAP_REFERENCE or CHILD_MAP_REFERENCE'
            }), 400
            
        conn = create_oracle_connection()
        try:
            cursor = conn.cursor()
            
            # Call the Oracle package procedure
            sql = """
            BEGIN
                TRG.PKGDWPRC.CREATE_JOB_DEPENDENCY(
                    p_parent_mapref => :parent_map_reference,
                    p_child_mapref => :child_map_reference
                );
            END;
            """
            
            # Execute the PL/SQL block
            cursor.execute(sql, {
                'parent_map_reference': parent_map_reference,
                'child_map_reference': child_map_reference
            })
            
            # Commit the transaction
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': 'Parent-child job relationship saved successfully'
            })
            
        except Exception as e:
            conn.rollback()
            error_message = str(e)
            print(f"Database error in save_parent_child_job: {error_message}")
            return jsonify({
                'success': False,
                'message': f'Database error: {error_message}'
            }), 500
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error in save_parent_child_job: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'An error occurred while saving the parent-child job relationship: {str(e)}'
        }), 500



# schedule job
@jobs_bp.route('/enable_disable_job', methods=['POST'])
def enable_disable_job():
 
    data = request.json
    map_ref = data.get('MAPREF')
    job_flag = data.get('JOB_FLG')
    conn = create_oracle_connection()
    try:
        cursor = conn.cursor()
        query = f""" 
        BEGIN
          {ORACLE_SCHEMA}.PKGDWPRC.ENABLE_DISABLE_SCHEDULE(:map_ref, :job_flag);
        END;
        """
        cursor.execute(query, {'map_ref': map_ref, 'job_flag': job_flag})
        conn.commit()
        if job_flag == 'E':
            return jsonify({
                'success': True,
                'message': 'Job enabled successfully'
            })
        elif job_flag == 'D':
            return jsonify({
                'success': True,
                'message': 'Job disabled successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Invalid job flag'
            }), 400 
    except Exception as e:
        conn.rollback()
        error_message = str(e)
        print(f"Database error in enable_disable_job: {error_message}")
        return jsonify({
            'success': False,
            'message': f'Database error: {error_message}'
        }), 500
    finally:
        conn.close()
        