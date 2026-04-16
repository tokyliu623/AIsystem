import os
import re
import time
import uuid
import json
import pandas as pd
import threading
import logging
import requests
import csv
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, send_file
from werkzeug.utils import secure_filename
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置简化的日志
logging.basicConfig(
    level=logging.WARNING,  # 调整为WARNING级别，减少INFO日志
    format='[%(asctime)s] [%(levelname)s] %(message)s',  # 简化格式，移除文件名和行号
    datefmt='%H:%M:%S'  # 只显示时间，不显示日期
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3  # 最大重试次数
API_TIMEOUT = 3000  # API超时时间（秒）
TIMEOUT = 3000 

# 在文件顶部添加常量
HISTORY_DIR = 'history'
HISTORY_INDEX_FILE = os.path.join(HISTORY_DIR, 'index.json')
os.makedirs(HISTORY_DIR, exist_ok=True)

app = Flask(__name__)

# 配置文件上传
UPLOAD_FOLDER = 'data'
RESULT_FOLDER = 'result'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# 确保目录存在
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

# API配置 - 更新为新的API地址
API_URL = "https://jiuwen-api.vmic.xyz/v1/chat-messages"

# 任务状态 - 使用全局字典存储，不依赖Flask session
task_status = {
    'comment': {},
    'cover': {},
    'push': {},  # 合并后的智能push巡检类型
    'brand': {},  # 品牌守护审核类型
    'news': {}   # 资讯巡检类型
}
# 历史记录
history_records = []

def allowed_file(filename):
    """检查文件类型是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_task_id(audit_type, session_id):
    """获取任务ID - 使用传入的session_id而非Flask session"""
    if session_id not in task_status[audit_type]:
        task_status[audit_type][session_id] = {
            'id': str(uuid.uuid4()),
            'status': 'idle',
            'progress': 0,
            'total': 0,
            'processed': 0,
            'paused': False,
            'message': '',
            'statistics': {
                'results': {},
                'tags': {}
            },
            'history': []
        }
    return task_status[audit_type][session_id]['id']

def update_task_status(audit_type, session_id, status=None, progress=None, total=None, processed=None, paused=None, message=None):
    """更新任务状态 - 使用传入的session_id而非Flask session"""
    if session_id in task_status[audit_type]:
        if status is not None:
            task_status[audit_type][session_id]['status'] = status
        if progress is not None:
            task_status[audit_type][session_id]['progress'] = progress
        if total is not None:
            task_status[audit_type][session_id]['total'] = total
        if processed is not None:
            task_status[audit_type][session_id]['processed'] = processed
        if paused is not None:
            task_status[audit_type][session_id]['paused'] = paused
        if message is not None:
            task_status[audit_type][session_id]['message'] = message
            # 记录历史消息
            task_status[audit_type][session_id]['history'].append({
                'time': datetime.now().strftime('%H:%M:%S'),
                'message': message,
                'status': status or task_status[audit_type][session_id]['status']
            })

def update_statistics(audit_type, session_id, result, tags):
    """更新统计数据 - 使用传入的session_id而非Flask session"""
    if session_id in task_status[audit_type]:
        # 更新结果统计
        if result in task_status[audit_type][session_id]['statistics']['results']:
            task_status[audit_type][session_id]['statistics']['results'][result] += 1
        else:
            task_status[audit_type][session_id]['statistics']['results'][result] = 1
        
        # 更新标签统计
        for tag in tags:
            if tag in task_status[audit_type][session_id]['statistics']['tags']:
                task_status[audit_type][session_id]['statistics']['tags'][tag] += 1
            else:
                task_status[audit_type][session_id]['statistics']['tags'][tag] = 1

def get_upload_path(audit_type, session_id):
    """获取上传文件路径 - 使用传入的session_id而非Flask session"""
    task_id = get_task_id(audit_type, session_id)
    return os.path.join(UPLOAD_FOLDER, "%s_%s.xlsx" % (audit_type, task_id))

def get_result_path(audit_type, session_id):
    """获取结果文件路径 - 使用传入的session_id而非Flask session"""
    task_id = get_task_id(audit_type, session_id)
    return os.path.join(RESULT_FOLDER, "%s_%s_result.xlsx" % (audit_type, task_id))

# 修改 add_to_history 函数
def add_to_history(audit_type, session_id, filename, total_rows, statistics):
    """添加到历史记录 - 使用文件系统持久化存储"""
    try:
        task_id = get_task_id(audit_type, session_id)
        result_path = get_result_path(audit_type, session_id)
        
        # 创建历史记录条目
        history_entry = {
            'id': task_id,
            'audit_type': audit_type,
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'result_path': result_path,
            'filename': filename,
            'total_rows': total_rows,
            'statistics': statistics,
            'status': 'completed'
        }
        
        # 加载现有索引
        history_index = []
        if os.path.exists(HISTORY_INDEX_FILE):
            try:
                with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
                    history_index = json.load(f)
            except:
                history_index = []
        
        # 添加新记录到索引
        history_index.append(history_entry)
        
        # 保存索引
        with open(HISTORY_INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(history_index, f, ensure_ascii=False, indent=2)
            
        # 同时保存详细记录到单独文件
        history_file = os.path.join(HISTORY_DIR, f"{task_id}.json")
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(history_entry, f, ensure_ascii=False, indent=2)
            
        return True
    except Exception as e:
        logger.error("保存历史记录失败: %s" % str(e))
        return False   

# 新增历史记录分页API
@app.route('/history/page')
def get_history_page():
    """获取分页历史记录"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        audit_type = request.args.get('audit_type', '')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        
        # 加载历史索引
        if not os.path.exists(HISTORY_INDEX_FILE):
            return jsonify({'history': [], 'total': 0, 'pages': 0})
            
        with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
            all_history = json.load(f)
        
        # 过滤记录
        filtered_history = []
        for record in all_history:
            # 按类型过滤
            if audit_type and record['audit_type'] != audit_type:
                continue
                
            # 按日期过滤
            record_date = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
            if start_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                if record_date < start_dt:
                    continue
            if end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
                if record_date >= end_dt:
                    continue
                    
            filtered_history.append(record)
        
        # 按时间倒序排序
        filtered_history.sort(key=lambda x: x['datetime'], reverse=True)
        
        # 分页
        total = len(filtered_history)
        pages = (total + per_page - 1) // per_page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paged_history = filtered_history[start_idx:end_idx]
        
        return jsonify({
            'history': paged_history,
            'total': total,
            'pages': pages,
            'page': page
        })
        
    except Exception as e:
        logger.error("获取分页历史记录错误: %s" % str(e))
        return jsonify({'error': '获取历史记录失败: %s' % str(e)}), 500

# 修复历史统计API
@app.route('/history/statistics')
def get_history_statistics():
    """获取历史统计信息 - 修复版，包含巡检量级统计并支持时间筛选"""
    try:
        # 获取时间筛选参数
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        
        # 加载历史索引
        if not os.path.exists(HISTORY_INDEX_FILE):
            return jsonify({
                'by_type': {}, 
                'by_date': {},
                'by_volume': {}
            })
            
        with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
            all_history = json.load(f)
        
        # 过滤记录（按时间筛选）
        filtered_history = []
        for record in all_history:
            # 按日期过滤
            record_date = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
            if start_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                if record_date < start_dt:
                    continue
            if end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
                if record_date >= end_dt:
                    continue
                    
            filtered_history.append(record)
        
        # 按类型统计任务数量
        by_type = {}
        for record in filtered_history:
            audit_type = record['audit_type']
            if audit_type not in by_type:
                by_type[audit_type] = 0
            by_type[audit_type] += 1
        
        # 按类型统计巡检量级（total_rows）
        by_volume = {}
        for record in filtered_history:
            audit_type = record['audit_type']
            total_rows = record.get('total_rows', 0)
            if audit_type not in by_volume:
                by_volume[audit_type] = 0
            by_volume[audit_type] += total_rows
        
        # 按日期统计（最近30天）
        by_date = {}
        today = datetime.now()
        for i in range(30):
            date_str = (today - timedelta(days=i)).strftime('%Y-%m-%d')
            by_date[date_str] = 0
            
        for record in filtered_history:
            record_date = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
            date_str = record_date.strftime('%Y-%m-%d')
            if date_str in by_date:
                by_date[date_str] += 1
        
        return jsonify({
            'by_type': by_type,
            'by_date': by_date,
            'by_volume': by_volume
        })
        
    except Exception as e:
        logger.error("获取历史统计错误: %s" % str(e))
        return jsonify({'error': '获取统计信息失败: %s' % str(e)}), 500

# 新增导出历史记录API
@app.route('/history/export')
def export_history():
    """导出历史记录"""
    try:
        export_format = request.args.get('format', 'json')
        audit_type = request.args.get('audit_type', '')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        
        # 加载和过滤历史记录（同上）
        if not os.path.exists(HISTORY_INDEX_FILE):
            return jsonify({'error': '没有历史记录可导出'}), 404
            
        with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
            all_history = json.load(f)
        
        filtered_history = []
        for record in all_history:
            if audit_type and record['audit_type'] != audit_type:
                continue
                
            record_date = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
            if start_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                if record_date < start_dt:
                    continue
            if end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
                if record_date >= end_dt:
                    continue
                    
            filtered_history.append(record)
        
        # 根据格式导出
        if export_format == 'json':
            export_path = os.path.join(HISTORY_DIR, f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(filtered_history, f, ensure_ascii=False, indent=2)
            return send_file(export_path, as_attachment=True)
            
        elif export_format == 'csv':
            export_path = os.path.join(HISTORY_DIR, f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            with open(export_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # 写入标题行
                writer.writerow(['ID', '任务类型', '执行时间', '文件名', '总行数', '正常数', '低质数', '处理失败数'])
                
                # 写入数据行
                for record in filtered_history:
                    stats = record.get('statistics', {})
                    results = stats.get('results', {})
                    writer.writerow([
                        record['id'],
                        record['audit_type'],
                        record['datetime'],
                        record.get('filename', ''),
                        record.get('total_rows', 0),
                        results.get('正常', 0),
                        results.get('低质', 0),
                        results.get('处理失败', 0)
                    ])
                    
            return send_file(export_path, as_attachment=True)
        else:
            return jsonify({'error': '不支持的导出格式'}), 400
            
    except Exception as e:
        logger.error("导出历史记录错误: %s" % str(e))
        return jsonify({'error': '导出失败: %s' % str(e)}), 500

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')
    
@app.route('/upload', methods=['POST'])
def upload_file():
    """上传文件"""
    try:
        # 获取参数
        audit_type = request.form.get('audit_type')
        session_id = request.form.get('session_id')
        
        if not audit_type or not session_id:
            return jsonify({'error': '审核类型和会话ID不能为空'}), 400
        
        # 修复审核类型验证
        valid_audit_types = ['comment', 'cover', 'push', 'brand', 'news']
        if audit_type not in valid_audit_types:
            return jsonify({
                'error': '无效的审核类型: %s' % audit_type,
                'valid_types': valid_audit_types
            }), 400
        
        # 确保所有审核类型都有完整的任务状态结构
        if session_id not in task_status[audit_type]:
            task_status[audit_type][session_id] = {
                'id': str(uuid.uuid4()),
                'status': 'idle',
                'progress': 0,
                'total': 0,
                'processed': 0,
                'paused': False,
                'message': '',
                'statistics': {
                    'results': {},
                    'tags': {}
                },
                'history': []
            }
            # 特殊字段初始化
            if audit_type == 'push':
                task_status[audit_type][session_id]['conversation_id'] = ''
                task_status[audit_type][session_id]['subtasks'] = {}
                task_status[audit_type][session_id]['completed_subtasks'] = 0
        
        if 'file' not in request.files:
            return jsonify({'error': '未找到文件'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': '未选择文件'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': '不支持的文件类型，请上传Excel文件'}), 400
        
        # 检查是否有正在进行的任务
        if session_id in task_status[audit_type]:
            current_status = task_status[audit_type][session_id]['status']
            if current_status in ['processing', 'done']:
                return jsonify({'error': '请先结束当前任务'}), 400
        
        # 保存文件
        filename = get_upload_path(audit_type, session_id)
        file.save(filename)
        
        # 更新任务状态
        update_task_status(audit_type, session_id, status='idle', progress=0, message='文件上传成功，请点击开始巡检')
        
        return jsonify({'message': '文件上传成功'})
        
    except Exception as e:
        logger.error("文件上传错误: %s" % str(e))
        return jsonify({'error': '文件上传失败: %s' % str(e)}), 500
            
@app.route('/run', methods=['POST'])
def run_task():
    """启动任务 - 修复 429 限速问题和多Agent逻辑"""
    try:
        # 获取参数
        data = request.get_json()
        if not data:
            return jsonify({'error': '请求数据为空'}), 400
            
        audit_type = data.get('audit_type')
        api_key = data.get('api_key', '')  # 单个API密钥（兼容旧版）
        api_keys = data.get('api_keys', [])  # 多个API密钥（新版多Agent）
        session_id = data.get('session_id')
        
        # 日志记录
        api_key_len = 0
        # ================ 修复：统一处理所有审核类型的API密钥 ================
        if audit_type in ['push', 'comment', 'cover', 'brand']:
            if isinstance(api_keys, list) and len(api_keys) > 0:
                api_key_len = len(api_keys)
            elif isinstance(api_key, str) and api_key.strip():
                api_key_len = 1
                api_keys = [api_key]  # 转换为列表格式
                
        logger.info("收到运行任务请求: audit_type=%s, session_id=%s, api_keys_count=%d" % 
                   (audit_type, session_id, api_key_len))
        
        if not audit_type:
            return jsonify({'error': '审核类型不能为空'}), 400
            
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:
            return jsonify({'error': '无效的审核类型: %s' % audit_type}), 400
        
        if not session_id:
            return jsonify({'error': '会话ID不能为空'}), 400
        
        # ================ 修复API密钥验证逻辑 ================
        # 问题 1 修复：移除网络请求验证，避免触发 429 错误
        # 仅保留非空检查
        if audit_type in ['comment', 'cover', 'push', 'brand']:
            # ================ 修复：统一验证逻辑 ================
            if not api_keys or len(api_keys) == 0:
                return jsonify({'error': 'API密钥不能为空'}), 400
            # 跳过 validate_api_key(api_key) 网络调用
            logger.info("API密钥格式校验通过，跳过网络验证以避免429")
                
        elif audit_type == 'news':
            # 资讯巡检需要3个API密钥
            if not api_key:
                return jsonify({'error': '资讯巡检需要3个API密钥，用|||分隔'}), 400
            # 简单校验格式
            if len(api_key.split('|||')) != 3:
                return jsonify({'error': '资讯巡检API密钥格式错误，需要3个密钥用|||分隔'}), 400
        
        # ================ 检查文件是否存在 ================
        filename = get_upload_path(audit_type, session_id)

        if not os.path.exists(filename):
            logger.error(f"文件不存在: {filename}")
            return jsonify({'error': '请先上传文件，文件不存在'}), 400
        
        # 更新任务状态
        update_task_status(audit_type, session_id, status='processing', progress=0, message='开始处理...')
        
        # 启动处理线程
        # ================ 修复：统一传递api_keys参数 ================
        if audit_type == 'comment':
            thread = threading.Thread(target=process_comment_file, args=(filename, api_keys, session_id))
        elif audit_type == 'cover':
            thread = threading.Thread(target=process_cover_file, args=(filename, api_keys, session_id))
        elif audit_type == 'push':
            thread = threading.Thread(target=process_push_file, args=(filename, api_keys, session_id))
        elif audit_type == 'brand':
            thread = threading.Thread(target=process_brand_file, args=(filename, api_keys, session_id))
        elif audit_type == 'news':
            thread = threading.Thread(target=process_news_file, args=(filename, api_key, session_id))
        
        thread.daemon = True
        thread.start()
        
        return jsonify({'message': '任务已启动'})
        
    except Exception as e:
        logger.error("启动任务错误: %s" % str(e))
        return jsonify({'error': '启动任务失败: %s' % str(e)}), 500
        
@app.route('/status/<audit_type>')
def get_status(audit_type):
    """获取任务状态"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:
            return jsonify({'error': '无效的审核类型'}), 400
        
        if session_id not in task_status[audit_type]:
            # 初始化任务状态
            get_task_id(audit_type, session_id)
        
        status_data = task_status[audit_type][session_id]
        
        return jsonify({
            'status': status_data['status'],
            'progress': status_data['progress'],
            'total': status_data['total'],
            'processed': status_data['processed'],
            'paused': status_data['paused'],
            'message': status_data['message'],
            'history': status_data['history'][-10:] if status_data['history'] else []
        })
        
    except Exception as e:
        logger.error("获取状态错误: %s" % str(e))
        return jsonify({'error': '获取状态失败: %s' % str(e)}), 500

@app.route('/control', methods=['POST'])
def control_task():
    """控制任务（暂停/继续/完成/结束）"""
    try:
        # 获取参数
        data = request.get_json()
        audit_type = data.get('audit_type')
        action = data.get('action')
        session_id = data.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:
            return jsonify({'error': '无效的审核类型'}), 400
        
        if action not in ['pause', 'resume', 'finish', 'end']:
            return jsonify({'error': '无效的操作'}), 400
        
        if session_id not in task_status[audit_type]:
            return jsonify({'error': '任务不存在'}), 404
        
        # 执行操作
        if action == 'pause':
            task_status[audit_type][session_id]['paused'] = True
            update_task_status(audit_type, session_id, message='任务已暂停')
            return jsonify({'message': '任务已暂停'})
            
        elif action == 'resume':
            task_status[audit_type][session_id]['paused'] = False
            update_task_status(audit_type, session_id, message='任务已继续')
            return jsonify({'message': '任务已继续'})
            
        elif action == 'finish':
            update_task_status(audit_type, session_id, status='done', message='任务已完成')
            return jsonify({'message': '任务已完成'})
            
        elif action == 'end':
            # 重置任务状态
            task_status[audit_type][session_id] = {
                'id': str(uuid.uuid4()),
                'status': 'idle',
                'progress': 0,
                'total': 0,
                'processed': 0,
                'paused': False,
                'message': '',
                'statistics': {
                    'results': {},
                    'tags': {}
                },
                'history': []
            }
            return jsonify({'message': '任务已结束'})
        
    except Exception as e:
        logger.error("控制任务错误: %s" % str(e))
        return jsonify({'error': '控制任务失败: %s' % str(e)}), 500

@app.route('/statistics/<audit_type>')
def get_statistics(audit_type):
    """获取统计数据"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:
            return jsonify({'error': '无效的审核类型'}), 400
        
        if session_id not in task_status[audit_type]:
            return jsonify({'error': '任务不存在'}), 404
        
        # 返回统计数据
        statistics = task_status[audit_type][session_id]['statistics']
        
        # 确保结果和标签统计不为空
        if not statistics['results']:
            statistics['results'] = {'无数据': 0}
        
        if not statistics['tags']:
            statistics['tags'] = {'无标签': 0}
        
        return jsonify(statistics)
        
    except Exception as e:
        logger.error("获取统计数据错误: %s" % str(e))
        return jsonify({'error': '获取统计数据失败: %s' % str(e)}), 500

@app.route('/download/<audit_type>')
def download_result(audit_type):
    """下载结果文件"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:
            return jsonify({'error': '无效的审核类型'}), 400
        
        # 获取结果文件路径
        result_path = get_result_path(audit_type, session_id)
        
        if not os.path.exists(result_path):
            return jsonify({'error': '结果文件不存在'}), 404
        
        # 返回文件
        return send_file(result_path, as_attachment=True)
        
    except Exception as e:
        logger.error("下载结果错误: %s" % str(e))
        return jsonify({'error': '下载结果失败: %s' % str(e)}), 500

@app.route('/history')
def get_history():
    """获取历史记录"""
    try:
        return jsonify({'history': history_records})
        
    except Exception as e:
        logger.error("获取历史记录错误: %s" % str(e))
        return jsonify({'error': '获取历史记录失败: %s' % str(e)}), 500

# 修复历史记录下载功能
@app.route('/history/download/<history_id>')
def download_history(history_id):
    """下载历史结果文件 - 修复版，从文件系统读取历史记录"""
    try:
        # 从文件系统加载历史记录
        if not os.path.exists(HISTORY_INDEX_FILE):
            return jsonify({'error': '历史记录不存在'}), 404
            
        with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
            all_history = json.load(f)
        
        # 查找历史记录
        target_record = None
        for record in all_history:
            if record['id'] == history_id:
                target_record = record
                break
        
        if not target_record:
            return jsonify({'error': '历史记录不存在'}), 404
        
        result_path = target_record['result_path']
        
        if not os.path.exists(result_path):
            return jsonify({'error': '历史结果文件不存在'}), 404
        
        # 返回文件
        return send_file(result_path, as_attachment=True)
        
    except Exception as e:
        logger.error("下载历史结果错误: %s" % str(e))
        return jsonify({'error': '下载历史结果失败: %s' % str(e)}), 500

# 删除历史记录路由 - 修复版
@app.route("/history/<history_id>", methods=["DELETE"])
def delete_history_record(history_id):
    """删除历史记录并更新统计数据"""
    try:
        # 加载历史索引
        if not os.path.exists(HISTORY_INDEX_FILE):
            return jsonify({"error": "历史记录索引文件不存在"}), 404

        with open(HISTORY_INDEX_FILE, "r", encoding="utf-8") as f:
            all_history = json.load(f)

        # 查找要删除的记录
        target_record = None
        for record in all_history:
            if record["id"] == history_id:
                target_record = record
                break

        if not target_record:
            return jsonify({"error": "未找到指定的历史记录"}), 404

        # 获取结果文件路径
        result_path = target_record.get('result_path', '')
        
        # 从索引中删除记录
        updated_history = [record for record in all_history if record["id"] != history_id]

        # 保存更新后的索引
        with open(HISTORY_INDEX_FILE, "w", encoding="utf-8") as f:
            json.dump(updated_history, f, ensure_ascii=False, indent=2)

        # 删除对应的详细历史记录文件
        history_file = os.path.join(HISTORY_DIR, f"{history_id}.json")
        if os.path.exists(history_file):
            os.remove(history_file)
            
        # 删除对应的结果文件 (如果存在)
        if result_path and os.path.exists(result_path):
            os.remove(result_path)

        return jsonify({"message": "历史记录删除成功", "id": history_id}), 200

    except Exception as e:
        logger.error(f"删除历史记录失败: {e}")
        return jsonify({"error": f"删除失败: {e}"}), 500

@app.route('/history/scan-and-restore', methods=['POST'])
def scan_and_restore_history():
    """扫描data和result目录，恢复缺失的历史记录"""
    try:
        import re
        from datetime import datetime as dt
        from collections import Counter
        
        pattern_data = re.compile(r'^(comment|cover|push|batch_push|brand|news)_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.xlsx$', re.IGNORECASE)
        pattern_result = re.compile(r'^(comment|cover|push|batch_push|brand|news)_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})_result\.xlsx$', re.IGNORECASE)
        
        data_files = {}
        if os.path.exists(UPLOAD_FOLDER):
            for fname in os.listdir(UPLOAD_FOLDER):
                m = pattern_data.match(fname)
                if m:
                    audit_type = m.group(1).lower()
                    task_id = m.group(2).lower()
                    data_files[task_id] = {'type': audit_type, 'filename': fname, 'path': os.path.join(UPLOAD_FOLDER, fname)}
        
        result_files = {}
        if os.path.exists(RESULT_FOLDER):
            for fname in os.listdir(RESULT_FOLDER):
                m = pattern_result.match(fname)
                if m:
                    audit_type = m.group(1).lower()
                    task_id = m.group(2).lower()
                    result_files[task_id] = {'type': audit_type, 'filename': fname, 'path': os.path.join(RESULT_FOLDER, fname)}
        
        all_task_ids = set(list(data_files.keys()) + list(result_files.keys()))
        
        existing_index = []
        if os.path.exists(HISTORY_INDEX_FILE):
            try:
                with open(HISTORY_INDEX_FILE, 'r', encoding='utf-8') as f:
                    existing_index = json.load(f)
            except:
                existing_index = []
        
        existing_ids = set(r.get('id', '').lower() for r in existing_index)
        
        new_tasks = all_task_ids - existing_ids
        restored = []
        
        for task_id in new_tasks:
            try:
                has_result = task_id in result_files
                has_data = task_id in data_files
                
                audit_type = None
                filename = ''
                result_path = ''
                file_mtime = None
                
                if has_result:
                    audit_type = result_files[task_id]['type']
                    filename = result_files[task_id]['filename']
                    result_path = result_files[task_id]['path']
                    file_mtime = os.path.getmtime(result_files[task_id]['path'])
                elif has_data:
                    audit_type = data_files[task_id]['type']
                    filename = data_files[task_id]['filename']
                    result_path = os.path.join(RESULT_FOLDER, "%s_%s_result.xlsx" % (audit_type, task_id))
                    file_mtime = os.path.getmtime(data_files[task_id]['path'])
                
                if not audit_type:
                    continue
                
                if file_mtime:
                    record_datetime = dt.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    record_datetime = dt.now().strftime('%Y-%m-%d %H:%M:%S')
                
                statistics = {'results': {}, 'tags': {}}
                total_rows = 0
                status = 'completed'
                
                if has_result:
                    try:
                        df = pd.read_excel(result_files[task_id]['path'], engine='openpyxl')
                        total_rows = len(df)
                        
                        result_col = None
                        for col_name in ['审核结果']:
                            if col_name in df.columns:
                                result_col = col_name
                                break
                        
                        if result_col:
                            result_counts = df[result_col].value_counts().to_dict()
                            statistics['results'] = {str(k): int(v) for k, v in result_counts.items()}
                        
                        tag_col = None
                        for col_name in ['违规标签', '低质标签']:
                            if col_name in df.columns:
                                tag_col = col_name
                                break
                        
                        if tag_col:
                            all_tags_list = []
                            for val in df[tag_col].dropna():
                                val_str = str(val).strip()
                                if val_str and val_str != '/':
                                    tags_split = [t.strip() for t in val_str.split(',') if t.strip()]
                                    all_tags_list.extend(tags_split)
                            
                            if all_tags_list:
                                tag_counts = Counter(all_tags_list)
                                statistics['tags'] = {str(k): int(v) for k, v in tag_counts.items()}
                    except Exception as e:
                        logger.warning("读取结果文件失败 %s: %s" % (result_files[task_id]['path'], str(e)))
                        
                elif has_data:
                    try:
                        df = pd.read_excel(data_files[task_id]['path'], engine='openpyxl')
                        total_rows = len(df)
                    except:
                        pass
                    status = 'partial'
                
                history_entry = {
                    'id': task_id,
                    'audit_type': audit_type,
                    'datetime': record_datetime,
                    'result_path': result_path,
                    'filename': filename.replace('_result.xlsx', '.xlsx') if '_result.xlsx' in filename else filename,
                    'total_rows': total_rows,
                    'statistics': statistics,
                    'status': status
                }
                
                existing_index.append(history_entry)
                
                history_file = os.path.join(HISTORY_DIR, f"{task_id}.json")
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(history_entry, f, ensure_ascii=False, indent=2)
                
                restored.append({
                    'id': task_id,
                    'type': audit_type,
                    'has_result': has_result,
                    'total_rows': total_rows
                })
                
            except Exception as e:
                logger.error("恢复任务 %s 失败: %s" % (task_id, str(e)))
                continue
        
        with open(HISTORY_INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_index, f, ensure_ascii=False, indent=2)
        
        return jsonify({
            'restored': len(restored),
            'details': restored,
            'message': '成功恢复 %d 条历史记录' % len(restored)
        })
        
    except Exception as e:
        logger.error("扫描恢复历史记录错误: %s" % str(e))
        return jsonify({'error': '扫描恢复失败: %s' % str(e)}), 500

# ================ 多Agent智能评论巡检功能 ================
def process_comment_file(filename, api_keys, session_id):
    """处理评论文件 - 多Agent版本"""
    try:
        # 读取Excel文件
        update_task_status('comment', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')
        
        # 检查必要的列
        if '评论内容' not in df.columns:
            update_task_status('comment', session_id, status='error', message='文件格式错误：缺少"评论内容"列')
            return
        
        # 数据清洗
        update_task_status('comment', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['评论内容'])
        df = df[df['评论内容'].astype(str).str.strip() != '']
        
        total_rows = len(df)
        if total_rows == 0:
            update_task_status('comment', session_id, status='error', message='文件中没有有效数据')
            return
        
        # 根据API密钥数量分割数据
        num_agents = min(len(api_keys), 10)  # 最多10个Agent
        chunk_size = max(1, total_rows // num_agents)
        chunks = []
        for i in range(0, total_rows, chunk_size):
            chunk_end = min(i + chunk_size, total_rows)
            chunks.append(df.iloc[i:chunk_end].copy())
        
        # 如果分块数超过Agent数量，合并最后两个块
        if len(chunks) > num_agents:
            last_chunk = chunks.pop()
            chunks[-1] = pd.concat([chunks[-1], last_chunk], ignore_index=True)
        
        # 确保正好num_agents个块
        while len(chunks) < num_agents:
            chunks.append(pd.DataFrame(columns=df.columns))
        
        update_task_status('comment', session_id, total=total_rows, 
                         message='数据准备完成，分为%d个子任务，开始处理 %d 条评论' % (len(chunks), total_rows))
        
        # 初始化子任务状态
        task_status['comment'][session_id]['subtasks'] = {}
        task_status['comment'][session_id]['completed_subtasks'] = 0
        
        # 创建线程池执行子任务
        with ThreadPoolExecutor(max_workers=num_agents) as executor:
            # 提交所有子任务
            future_to_chunk = {}
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
                    # 使用对应的API密钥
                    api_key = api_keys[i] if i < len(api_keys) else api_keys[0]
                    future = executor.submit(process_comment_chunk, chunk, api_key, session_id, i)
                    future_to_chunk[future] = i
                    # 初始化子任务状态
                    task_status['comment'][session_id]['subtasks'][i] = {
                        'status': 'processing',
                        'progress': 0,
                        'total': len(chunk),
                        'processed': 0
                    }
            
            # 收集结果
            completed_chunks = []
            total_futures = len(future_to_chunk)
            
            for i, future in enumerate(as_completed(future_to_chunk)):
                chunk_index = future_to_chunk[future]
                try:
                    result_chunk = future.result()
                    completed_chunks.append(result_chunk)
                    
                    # 更新子任务完成状态
                    task_status['comment'][session_id]['completed_subtasks'] += 1
                    completed_count = task_status['comment'][session_id]['completed_subtasks']
                    
                    # 更新总体进度
                    progress = int((completed_count / total_futures) * 100)
                    update_task_status('comment', session_id, progress=progress, 
                                     message='子任务 %d/%d 已完成，总体进度 %d%%' % (completed_count, total_futures, progress))
                    
                    # 更新子任务状态为完成
                    task_status['comment'][session_id]['subtasks'][chunk_index]['status'] = 'completed'
                    task_status['comment'][session_id]['subtasks'][chunk_index]['progress'] = 100
                    
                except Exception as e:
                    logger.error("评论子任务 %d 处理失败: %s" % (chunk_index, str(e)))
                    update_task_status('comment', session_id, 
                                     message='子任务 %d 处理异常: %s' % (chunk_index, str(e)), 
                                     status='warning')
                    # 标记子任务失败
                    task_status['comment'][session_id]['subtasks'][chunk_index]['status'] = 'error'
        
        # 合并所有子任务的结果
        if completed_chunks:
            final_df = pd.concat(completed_chunks, ignore_index=True)
            
            # 保存最终结果
            result_path = get_result_path('comment', session_id)
            final_df.to_excel(result_path, index=False)
            
            # 更新任务状态
            update_task_status('comment', session_id, status='done', progress=100, 
                             message='多Agent评论审核完成，请点击完成按钮')
            
            # 添加到历史记录
            add_to_history('comment', session_id, os.path.basename(filename), total_rows, 
                          task_status['comment'][session_id]['statistics'])
        else:
            update_task_status('comment', session_id, status='error', 
                             message='所有子任务处理失败')
        
    except Exception as e:
        logger.error("评论处理错误: %s" % str(e))
        update_task_status('comment', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也尝试记录历史
        try:
            add_to_history('comment', session_id, os.path.basename(filename), 
                          total_rows if 'total_rows' in locals() else 0, 
                          task_status['comment'][session_id]['statistics'])
        except:
            pass

def process_comment_chunk(chunk_df, api_key, session_id, chunk_index):
    """处理单个评论数据块"""
    try:
        # 复制数据块避免修改原数据
        df = chunk_df.copy()
        
        # 初始化结果列
        df['审核结果'] = ''
        df['违规标签'] = ''
        df['审核时间'] = ''
        df['处理批次'] = chunk_index + 1  # 标记处理批次
        
        # 更新子任务状态
        task_status['comment'][session_id]['subtasks'][chunk_index] = {
            'status': 'processing',
            'progress': 0,
            'total': len(df),
            'processed': 0
        }
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查主任务状态
                if (session_id in task_status['comment'] and 
                    task_status['comment'][session_id]['status'] != 'processing'):
                    break
                
                # 处理内容
                comment = str(row['评论内容']).strip()
                result, tags = process_comment(comment, api_key)
                
                # 特殊处理：如果标签为"/"，则结果应为"正常"
                if len(tags) == 0 or (len(tags) == 1 and tags[0] == '/'):
                    result = '正常'
                    tags = []
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '违规标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('comment', session_id, result, tags if tags else [])
                
                # 更新子任务进度
                processed_count = index + 1
                chunk_progress = int((processed_count / len(df)) * 100)
                task_status['comment'][session_id]['subtasks'][chunk_index]['processed'] = processed_count
                task_status['comment'][session_id]['subtasks'][chunk_index]['progress'] = chunk_progress
                
                # 添加间隔，避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                logger.error("评论处理错误(批次%d, 行%d): %s" % (chunk_index, index, str(e)))
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('comment', session_id, '处理失败', [])
                
                # 更新子任务进度
                processed_count = index + 1
                task_status['comment'][session_id]['subtasks'][chunk_index]['processed'] = processed_count
                
                continue
        
        # 标记子任务完成
        task_status['comment'][session_id]['subtasks'][chunk_index]['status'] = 'completed'
        task_status['comment'][session_id]['subtasks'][chunk_index]['progress'] = 100
        
        return df
        
    except Exception as e:
        logger.error("评论数据块处理错误(批次%d): %s" % (chunk_index, str(e)))
        # 返回原始数据块，标记为处理失败
        df = chunk_df.copy()
        df['审核结果'] = '处理失败'
        df['违规标签'] = '/'
        df['审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['处理批次'] = chunk_index + 1
        # 标记子任务失败
        task_status['comment'][session_id]['subtasks'][chunk_index]['status'] = 'error'
        return df

def process_comment(comment, api_key):
    """处理单条评论 - 修复版本，解决API结果解析问题"""
    # 最大重试次数
    max_retries = 3
    retry_count = 0
    # 设置超时时间（连接超时+读取超时）
    api_timeout = (10, 3000)  # (连接超时10秒, 读取超时3000秒)
    
    while retry_count < max_retries:
        try:
            # 构建请求数据
            data = {
                "query": "请审核以下评论内容是否低质，并给出审核结果和低质标签：\n\n%s" % comment,
                "inputs": {},
                "response_mode": "blocking",
                "user": "audit_system"
            }
            
            # 发送请求
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % api_key
            }
            
            logger.info("评论审核请求数据: %s" % json.dumps(data))
            
            # 发送请求，添加3000秒超时机制
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=api_timeout
            )
            
            logger.info("评论审核响应状态: %d" % response.status_code)
            
            # 处理非200状态码
            if response.status_code != 200:
                logger.error("评论审核响应错误: %s" % response.text)
                # 特殊处理501错误
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    logger.warning("501错误触发重试 (%d/%d)" % (retry_count, max_retries))
                    time.sleep(2)
                    continue
                # 其他非200状态码直接引发异常
                response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            logger.info("评论审核原始响应: %s" % assistant_message)
            
            # 解析API返回结果
            result, tags = parse_audit_result(assistant_message)
            
            logger.info("评论审核解析结果: %s, 标签: %s" % (result, tags))
            return result, tags
            
        except requests.exceptions.Timeout as timeout_err:
            # 专门处理超时异常
            retry_count += 1
            timeout_type = "连接" if "connect" in str(timeout_err).lower() else "读取"
            logger.error("API请求超时 (%s) (尝试 %d/%d): %s" % (timeout_type, retry_count, max_retries, str(timeout_err)))
            
            if retry_count >= max_retries:
                logger.critical("API请求达到最大超时重试次数")
                return '处理失败', []
            
            # 指数退避策略
            sleep_time = 2 ** retry_count
            logger.info("将在 %d 秒后重试..." % sleep_time)
            time.sleep(sleep_time)
            
        except requests.exceptions.RequestException as req_err:
            # 处理其他网络请求异常
            retry_count += 1
            logger.error("网络请求异常 (尝试 %d/%d): %s" % (retry_count, max_retries, str(req_err)))
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
            
        except Exception as e:
            # 处理其他未预期的异常
            retry_count += 1
            logger.error("未处理的异常 (尝试 %d/%d): %s" % (retry_count, max_retries, str(e)))
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
    
    return '处理失败', []

# ================ 多Agent智能Push巡检功能 ================
def process_push_file(filename, api_keys, session_id):
    """处理智能Push文件 - 多Agent版本"""
    try:
        # 读取Excel文件
        update_task_status('push', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')
        
        # 检查必要的列
        if '标题' not in df.columns or '摘要' not in df.columns:
            update_task_status('push', session_id, status='error', message='文件格式错误：缺少"标题"或"摘要"列')
            return
        
        # 数据清洗
        update_task_status('push', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['标题', '摘要'], how='all')
        
        total_rows = len(df)
        if total_rows == 0:
            update_task_status('push', session_id, status='error', message='文件中没有有效数据')
            return
        
        # 根据API密钥数量分割数据
        num_agents = min(len(api_keys), 10)  # 最多10个Agent
        chunk_size = max(1, total_rows // num_agents)
        chunks = []
        for i in range(0, total_rows, chunk_size):
            chunk_end = min(i + chunk_size, total_rows)
            chunks.append(df.iloc[i:chunk_end].copy())
        
        # 如果分块数超过Agent数量，合并最后两个块
        if len(chunks) > num_agents:
            last_chunk = chunks.pop()
            chunks[-1] = pd.concat([chunks[-1], last_chunk], ignore_index=True)
        
        # 确保正好num_agents个块
        while len(chunks) < num_agents:
            chunks.append(pd.DataFrame(columns=df.columns))
        
        update_task_status('push', session_id, total=total_rows, 
                         message='数据准备完成，分为%d个子任务，开始处理 %d 条Push内容' % (len(chunks), total_rows))
        
        # 初始化子任务状态
        task_status['push'][session_id]['subtasks'] = {}
        task_status['push'][session_id]['completed_subtasks'] = 0
        
        # 创建线程池执行子任务
        with ThreadPoolExecutor(max_workers=num_agents) as executor:
            # 提交所有子任务
            future_to_chunk = {}
            for i, chunk in enumerate(chunks):
                if len(chunk) > 0:
                    # 使用对应的API密钥
                    api_key = api_keys[i] if i < len(api_keys) else api_keys[0]
                    future = executor.submit(process_push_chunk, chunk, api_key, session_id, i)
                    future_to_chunk[future] = i
                    # 初始化子任务状态
                    task_status['push'][session_id]['subtasks'][i] = {
                        'status': 'processing',
                        'progress': 0,
                        'total': len(chunk),
                        'processed': 0
                    }
            
            # 收集结果
            completed_chunks = []
            total_futures = len(future_to_chunk)
            
            for i, future in enumerate(as_completed(future_to_chunk)):
                chunk_index = future_to_chunk[future]
                try:
                    result_chunk = future.result()
                    completed_chunks.append(result_chunk)
                    
                    # 更新子任务完成状态
                    task_status['push'][session_id]['completed_subtasks'] += 1
                    completed_count = task_status['push'][session_id]['completed_subtasks']
                    
                    # 更新总体进度
                    progress = int((completed_count / total_futures) * 100)
                    update_task_status('push', session_id, progress=progress, 
                                     message='子任务 %d/%d 已完成，总体进度 %d%%' % (completed_count, total_futures, progress))
                    
                    # 更新子任务状态为完成
                    task_status['push'][session_id]['subtasks'][chunk_index]['status'] = 'completed'
                    task_status['push'][session_id]['subtasks'][chunk_index]['progress'] = 100
                    
                except Exception as e:
                    logger.error("Push子任务 %d 处理失败: %s" % (chunk_index, str(e)))
                    update_task_status('push', session_id, 
                                     message='子任务 %d 处理异常: %s' % (chunk_index, str(e)), 
                                     status='warning')
                    # 标记子任务失败
                    task_status['push'][session_id]['subtasks'][chunk_index]['status'] = 'error'
        
        # 合并所有子任务的结果
        if completed_chunks:
            final_df = pd.concat(completed_chunks, ignore_index=True)
            
            # 保存最终结果
            result_path = get_result_path('push', session_id)
            final_df.to_excel(result_path, index=False)
            
            # 更新任务状态
            update_task_status('push', session_id, status='done', progress=100, 
                             message='多Agent智能Push审核完成，请点击完成按钮')
            
            # 添加到历史记录
            add_to_history('push', session_id, os.path.basename(filename), total_rows, 
                          task_status['push'][session_id]['statistics'])
        else:
            update_task_status('push', session_id, status='error', 
                             message='所有子任务处理失败')
        
    except Exception as e:
        logger.error("Push处理错误: %s" % str(e))
        update_task_status('push', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也尝试记录历史
        try:
            add_to_history('push', session_id, os.path.basename(filename), 
                          total_rows if 'total_rows' in locals() else 0, 
                          task_status['push'][session_id]['statistics'])
        except:
            pass

def process_push_chunk(chunk_df, api_key, session_id, chunk_index):
    """处理单个Push数据块"""
    try:
        # 复制数据块避免修改原数据
        df = chunk_df.copy()
        
        # 初始化结果列
        df['审核结果'] = ''
        df['低质标签'] = ''
        df['审核时间'] = ''
        df['处理批次'] = chunk_index + 1  # 标记处理批次
        
        # 初始化会话ID
        conversation_id = ''
        
        # 更新子任务状态
        task_status['push'][session_id]['subtasks'][chunk_index] = {
            'status': 'processing',
            'progress': 0,
            'total': len(df),
            'processed': 0
        }
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查主任务状态
                if (session_id in task_status['push'] and 
                    task_status['push'][session_id]['status'] != 'processing'):
                    break
                
                # 处理内容
                title = str(row['标题']).strip()
                summary = str(row['摘要']).strip()
                
                # 审核内容
                response = audit_content_batch(title, summary, api_key, conversation_id)
                result = response.get('result', '处理失败')
                tags = response.get('tags', [])
                conversation_id = response.get('conversation_id', conversation_id)
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '低质标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('push', session_id, result, tags if tags else [])
                
                # 更新子任务进度
                processed_count = index + 1
                chunk_progress = int((processed_count / len(df)) * 100)
                task_status['push'][session_id]['subtasks'][chunk_index]['processed'] = processed_count
                task_status['push'][session_id]['subtasks'][chunk_index]['progress'] = chunk_progress
                
                # 添加间隔，避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                logger.error("Push处理错误(批次%d, 行%d): %s" % (chunk_index, index, str(e)))
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '低质标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('push', session_id, '处理失败', [])
                
                # 更新子任务进度
                processed_count = index + 1
                task_status['push'][session_id]['subtasks'][chunk_index]['processed'] = processed_count
                
                continue
        
        # 标记子任务完成
        task_status['push'][session_id]['subtasks'][chunk_index]['status'] = 'completed'
        task_status['push'][session_id]['subtasks'][chunk_index]['progress'] = 100
        
        return df
        
    except Exception as e:
        logger.error("Push数据块处理错误(批次%d): %s" % (chunk_index, str(e)))
        # 返回原始数据块，标记为处理失败
        df = chunk_df.copy()
        df['审核结果'] = '处理失败'
        df['低质标签'] = '/'
        df['审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['处理批次'] = chunk_index + 1
        # 标记子任务失败
        task_status['push'][session_id]['subtasks'][chunk_index]['status'] = 'error'
        return df

def audit_content_batch(title, summary, api_key, conversation_id=''):
    """执行双字段内容审核（批量版本）"""
    headers = {
        "Authorization": "Bearer %s" % api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": sanitize_fields(title, summary),
        "inputs": {},
        "user": "PUSH_AUDIT_BOT",
        "conversation_id": conversation_id,
        "response_mode": "blocking"
    }
    
    session = create_retry_session()
    try:
        response = session.post(
            API_URL,
            headers=headers,
            json=payload,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        
        response_data = response.json()
        new_conversation_id = response_data.get("conversation_id", '')
        answer = response_data.get("answer", "")
        
        result, tags = parse_audit_result_push(answer)
        
        return {
            'result': result,
            'tags': tags,
            'conversation_id': new_conversation_id
        }
        
    except requests.exceptions.Timeout:
        logger.error("请求超时: %s..." % title[:50])
        return {"result": "请求超时", "tags": [], "conversation_id": conversation_id}
    except Exception as e:
        logger.error("服务异常: %s" % str(e))
        return {"result": "服务异常", "tags": [], "conversation_id": conversation_id}

def sanitize_fields(title, summary):
    """双字段消毒处理"""
    def clean_text(text):
        if pd.isnull(text):
            return ""
        return re.sub(r'[\x00-\x1F\\"{}]', '', str(text))[:1500]
    
    clean_title = clean_text(title)
    clean_summary = clean_text(summary)
    return "标题：%s\n摘要：%s" % (clean_title, clean_summary)[:3000]

def parse_audit_result_push(assistant_message):
    """精准解析审核结果（增强版）"""
        
    # 最大重试次数
    MAX_RETRIES = 3
    TIMEOUT = 1500 
    try:
        # 1. 过滤think标签内容
        think_pattern = r'<think>.*?</think>'
        filtered_message = re.sub(think_pattern, '', assistant_message, flags=re.DOTALL).strip()
        logger.info("过滤后内容: %s..." % filtered_message[:200])
        
        # 2. 定义多种解析模式
        patterns = [
            # 模式1：带编号的标准格式
            (r'（1）\s*审核结果\s*[:：]\s*(\S+)', r'（2）\s*低质标签\s*[:：]\s*(.+?)(?=\n|$)'),
            # 模式2：不带编号的格式
            (r'审核结果\s*[:：]\s*(\S+)', r'低质标签\s*[:：]\s*(.+?)(?=\n|$)'),
            # 模式3：简化格式
            (r'(?:审核结果|结果)\s*[:：]?\s*(正常|低质|违规)', r'(?:低质标签|标签|违规标签)\s*[:：]?\s*(.+?)(?=\n|$)')
        ]
        
        result = "解析失败"
        tags = []
        
        # 3. 尝试每种模式
        for result_pattern, tag_pattern in patterns:
            result_match = re.search(result_pattern, filtered_message, re.IGNORECASE)
            tag_match = re.search(tag_pattern, filtered_message, re.IGNORECASE)
            
            if result_match and tag_match:
                result = result_match.group(1).strip()
                tag_str = tag_match.group(1).strip()
                tags = parse_tags(tag_str)
                logger.info("匹配成功: 结果=%s, 标签=%s" % (result, tags))
                break
        
        # 4. 备用策略：关键词匹配
        if result == "解析失败":
            if '正常' in filtered_message and '违规' not in filtered_message and '低质' not in filtered_message:
                result = '正常'
            elif '低质' in filtered_message or '违规' in filtered_message:
                result = '低质'
            logger.info("关键词匹配: 结果=%s" % result)
        
        # 5. 后处理
        if result == '正常':
            tags = []  # 正常内容不应有标签
            
        return result, tags
        
    except Exception as e:
        logger.error("解析异常: %s" % str(e))
        return ("解析失败", [])

def create_retry_session():
    """创建带重试机制的请求会话"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https', adapter)
    return session

def parse_audit_result(assistant_message):
    """解析审核结果 - 修复版本，支持多种格式和增强错误处理"""
    result = "解析失败"
    tags = []
    
    try:
        # 第一步：过滤think标签内容
        think_pattern = r'<think>.*?</think>'
        filtered_message = re.sub(think_pattern, '', assistant_message, flags=re.DOTALL).strip()
        
        logger.info("过滤think标签后的内容: %s" % filtered_message)
        
        # 第二步：尝试多种正则表达式模式解析结果
        
        # 模式1：带编号的标准格式 （1）审核结果：xxx （2）低质标签：xxx
        pattern1_result = r'（1）\s*审核结果\s*[:：]\s*(\S+)'
        pattern1_tag = r'（2）\s*低质标签\s*[:：]\s*(.+?)(?=\n|$)'
        
        # 模式2：不带编号的格式 审核结果：xxx 低质标签：xxx
        pattern2_result = r'审核结果\s*[:：]\s*(\S+)'
        pattern2_tag = r'低质标签\s*[:：]\s*(.+?)(?=\n|$)'
        
        # 模式3：简化格式，只有结果和标签值
        pattern3_result = r'(?:审核结果|结果)\s*[:：]?\s*(正常|低质|违规)'
        pattern3_tag = r'(?:低质标签|标签|违规标签)\s*[:：]?\s*(.+?)(?=\n|$)'
        
        # 尝试解析结果
        result_patterns = [pattern1_result, pattern2_result, pattern3_result]
        tag_patterns = [pattern1_tag, pattern2_tag, pattern3_tag]
        
        result_found = False
        tag_found = False
        
        # 尝试每种模式
        for i, (result_pattern, tag_pattern) in enumerate(zip(result_patterns, tag_patterns)):
            if not result_found:
                result_match = re.search(result_pattern, filtered_message, re.IGNORECASE)
                if result_match:
                    result = result_match.group(1).strip()
                    result_found = True
                    logger.info("使用模式%d成功解析结果: %s" % (i+1, result))
            
            if not tag_found:
                tag_match = re.search(tag_pattern, filtered_message, re.IGNORECASE)
                if tag_match:
                    tag_str = tag_match.group(1).strip()
                    tags = parse_tags(tag_str)
                    tag_found = True
                    logger.info("使用模式%d成功解析标签: %s" % (i+1, tags))
        
        # 第三步：备用解析策略 - 基于关键词匹配
        if not result_found:
            if '正常' in filtered_message and ('违规' not in filtered_message and '低质' not in filtered_message):
                result = '正常'
                result_found = True
                logger.info("使用关键词匹配解析结果: 正常")
            elif '低质' in filtered_message or '违规' in filtered_message:
                result = '低质'
                result_found = True
                logger.info("使用关键词匹配解析结果: 低质")
        
        # 第四步：特殊处理逻辑
        # 如果结果是正常，确保标签为空
        if result == '正常':
            tags = []
        
        # 如果标签只有"/"或"无"等，清空标签列表
        if len(tags) == 1 and tags[0] in ['/', '无', '无标签', '']:
            tags = []
        
        # 如果结果是低质但没有标签，尝试从内容中提取
        if result == '低质' and not tags:
            tags = extract_tags_from_content(filtered_message)
        
        logger.info("最终解析结果: 结果=%s, 标签=%s" % (result, tags))
        
    except Exception as e:
        logger.error("解析审核结果时发生异常: %s" % str(e))
        result = "处理失败"
        tags = []
    
    return result, tags

def parse_tags(tag_str):
    """解析标签字符串，返回标签列表"""
    if not tag_str or tag_str.strip() in ['/', '无', '无标签', '']:
        return []
    
    # 清理标签字符串，支持多种分隔符
    tag_str = tag_str.replace('，', ',').replace('、', ',').replace('；', ',').replace(';', ',')
    tag_str = tag_str.replace('/', '').strip()
    
    if not tag_str:
        return []
    
    # 分割标签
    tags = [tag.strip() for tag in tag_str.split(',') if tag.strip()]
    
    # 过滤无效标签
    valid_tags = []
    for tag in tags:
        if tag and tag not in ['/', '无', '无标签', '']:
            valid_tags.append(tag)
    
    return valid_tags

def extract_tags_from_content(content):
    """从内容中提取可能的违规标签"""
    # 定义可能的标签关键词
    tag_keywords = {
        '涉政': ['涉政', '政治', '政策'],
        '违禁': ['违禁', '非法'],
        '色情': ['色情', '性'],
        '低俗': ['低俗', '低级'],
        '广告': ['广告', '推广'],
        '谩骂': ['谩骂', '辱骂', '歧视'],
        '灌水': ['灌水', '无意义']
    }
    
    found_tags = []
    content_lower = content.lower()
    
    for tag, keywords in tag_keywords.items():
        for keyword in keywords:
            if keyword in content_lower:
                found_tags.append(tag)
                break
    
    return found_tags

# ================ 其他巡检功能保持不变 ================

def process_cover_file(filename, api_keys, session_id):
    """处理封面文件 - 修复版，使用api_keys列表"""
    try:
        # 确保api_keys不为空
        if not api_keys or len(api_keys) == 0:
            update_task_status('cover', session_id, status='error', message='API密钥不能为空')
            return
            
        api_key = api_keys[0]  # 使用第一个密钥
        
        # 读取Excel文件
        update_task_status('cover', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')
        
        # 检查必要的列
        if '封面链接' not in df.columns:
            update_task_status('cover', session_id, status='error', message='文件格式错误：缺少"封面链接"列')
            return
        
        # 数据清洗
        update_task_status('cover', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['封面链接'])
        df = df[df['封面链接'].astype(str).str.strip() != '']
        
        # 初始化结果列
        df['审核结果'] = ''
        df['违规标签'] = ''
        df['审核时间'] = ''
        
        total_rows = len(df)
        update_task_status('cover', session_id, total=total_rows, message='数据准备完成，开始处理 %d 条封面链接' % total_rows)
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查是否暂停
                while task_status['cover'][session_id]['paused']:
                    time.sleep(0.5)
                    # 检查是否已结束任务
                    if task_status['cover'][session_id]['status'] == 'idle':
                        return
                
                # 检查任务状态
                if task_status['cover'][session_id]['status'] != 'processing':
                    break
                
                # 更新进度
                processed = index + 1
                progress = int((processed / total_rows) * 100)
                update_task_status('cover', session_id, progress=progress, processed=processed, message='开始处理项目 #%d/%d' % (index+1, total_rows))
                
                # 处理封面
                cover_url = row['封面链接']
                result, tags = process_cover(cover_url, api_key, index, session_id)
                
                # 特殊处理：如果标签为"/"，则结果应为"正常"
                if len(tags) == 0 or (len(tags) == 1 and tags[0] == '/'):
                    result = '正常'
                    tags = []
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '违规标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('cover', session_id, result, tags if tags else [])
                
                # 每条处理完成后保存一次结果，确保不丢失进度
                result_path = get_result_path('cover', session_id)
                df.to_excel(result_path, index=False)
                
                # 添加处理完成日志
                update_task_status('cover', session_id, message='项目 #%d/%d 处理完成，结果: %s' % (index+1, total_rows, result), status='processing')
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error("封面处理项目 #%d 错误: %s" % (index, str(e)))
                update_task_status('cover', session_id, message='项目 #%d 处理异常: %s，继续处理下一项' % (index+1, str(e)), status='warning')
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('cover', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('cover', session_id)
                df.to_excel(result_path, index=False)
                
                # 继续处理下一条，不中断循环
                continue
        
        # 保存最终结果
        result_path = get_result_path('cover', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('cover', session_id, status='done', progress=100, message='封面审核完成，请点击完成按钮')
      
        # 在函数末尾，修改 add_to_history 调用
        add_to_history('cover', session_id, os.path.basename(filename), total_rows, 
                      task_status['cover'][session_id]['statistics'])
        
    except Exception as e:
        logger.error("封面处理错误: %s" % str(e))
        update_task_status('cover', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也记录历史
        add_to_history('cover', session_id, os.path.basename(filename), total_rows, 
                      task_status['cover'][session_id]['statistics'])

def process_cover(cover_url, api_key, index, session_id):
    """处理单条封面链接 - 适配新的API接口"""
    # 应用速率限制
    update_task_status('cover', session_id, message='项目 #%d 应用速率限制...' % (index+1))
    time.sleep(1)  # 确保请求间隔至少1秒
    
    # 最大重试次数
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 构建请求数据 - 修改为新的API请求格式
            data = {
                "query": "请审核以下封面图片是否违规，并给出审核结果和违规标签：",
                "inputs": {},
                "response_mode": "blocking",
                "user": "audit_system",
                "upload_mediums": [
                    {
                        "url": cover_url,
                        "type": "image"
                    }
                ]
            }
            
            # 发送请求
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % api_key  # 保持Bearer和密钥之间的空格
            }
            
            # 打印请求数据用于调试
            logger.info("封面审核请求数据: %s" % json.dumps(data))
            
            # 记录开始时间
            start_time = time.time()
            
            # 发送请求
            update_task_status('cover', session_id, message='项目 #%d 发送请求 (尝试 %d/%d)...' % (index+1, retry_count+1, max_retries))
            response = requests.post(API_URL, headers=headers, json=data)
            
            # 打印响应状态和内容用于调试
            logger.info("封面审核响应状态: %d" % response.status_code)
            if response.status_code != 200:
                logger.error("封面审核响应错误: %s" % response.text)
                
                # 特殊处理501错误（对话ID不存在）
                if response.status_code == 501 and "conversation_id" in response.text:
                    # 重新尝试，不使用conversation_id
                    retry_count += 1
                    logger.info("封面审核重试 %d/%d" % (retry_count, max_retries))
                    time.sleep(2)  # 等待2秒后重试
                    continue
            
            response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 保存conversation_id以便后续使用
            conversation_id = result_data.get('conversation_id', '')
            logger.info("获取到conversation_id: %s" % conversation_id)
            
            # 提取审核结果和标签
            result = '正常'  # 默认为正常
            tags = []
            
            # 优先检查是否包含"正常"关键词
            if '正常' in assistant_message and '违规' not in assistant_message and '不合规' not in assistant_message:
                result = '正常'
                # 正常结果不需要提取标签
            elif '违规' in assistant_message or '不合规' in assistant_message:
                result = '违规'
                # 提取标签
                tag_indicators = ['标签：', '标签:', '违规标签：', '违规标签:']
                for indicator in tag_indicators:
                    if indicator in assistant_message:
                        tag_part = assistant_message.split(indicator, 1)[1].strip()
                        tag_list = tag_part.split('\n')[0].split('，')
                        # 也支持英文逗号分隔
                        if len(tag_list) == 1 and ',' in tag_list[0]:
                            tag_list = tag_list[0].split(',')
                        tags = [tag.strip() for tag in tag_list if tag.strip() and tag.strip() != '/']
                        break
            
            # 记录解析结果
            logger.info("封面审核结果: %s, 标签: %s" % (result, tags))
            return result, tags
            
        except requests.exceptions.Timeout as timeout_err:
            retry_count += 1
            timeout_type = "连接" if "connect" in str(timeout_err).lower() else "读取"
            logger.error("API请求超时 (%s) (尝试 %d/%d): %s" % (timeout_type, retry_count, max_retries, str(timeout_err)))
            
            if retry_count >= max_retries:
                logger.critical("API请求达到最大超时重试次数")
                return '处理失败', []
            
            sleep_time = 2 ** retry_count
            logger.info("将在 %d 秒后重试..." % sleep_time)
            time.sleep(sleep_time)
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            logger.error("网络请求异常 (尝试 %d/%d): %s" % (retry_count, max_retries, str(req_err)))
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
            logger.error("未处理的异常 (尝试 %d/%d): %s" % (retry_count, max_retries, str(e)))
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
    
    return '处理失败', []


# ================ 品牌守护审核功能 ================

def process_brand_file(filename, api_keys, session_id):
    """处理品牌守护文件 - 修复版，使用api_keys列表"""
    try:
        # 确保api_keys不为空
        if not api_keys or len(api_keys) == 0:
            update_task_status('brand', session_id, status='error', message='API密钥不能为空')
            return
            
        api_key = api_keys[0]  # 使用第一个密钥
        
        # 读取Excel文件
        update_task_status('brand', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')
        
        # 检查必要的列
        if '品牌标题' not in df.columns:
            update_task_status('brand', session_id, status='error', message='文件格式错误：缺少"品牌标题"列')
            return
        
        # 数据清洗
        update_task_status('brand', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['品牌标题'])
        df = df[df['品牌标题'].astype(str).str.strip() != '']
        
        # 初始化结果列
        df['审核结果'] = ''
        df['违规标签'] = ''
        df['审核时间'] = ''
        
        total_rows = len(df)
        update_task_status('brand', session_id, total=total_rows, message='数据准备完成，开始处理 %d 条品牌内容' % total_rows)
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查是否暂停
                while task_status['brand'][session_id]['paused']:
                    time.sleep(0.5)
                    if task_status['brand'][session_id]['status'] == 'idle':
                        return
                
                # 检查任务状态
                if task_status['brand'][session_id]['status'] != 'processing':
                    break
                
                # 更新进度
                processed = index + 1
                progress = int((processed / total_rows) * 100)
                update_task_status('brand', session_id, progress=progress, processed=processed, 
                                  message='开始处理品牌内容 #%d/%d' % (index+1, total_rows))
                
                # 处理内容
                content = row['品牌标题']
                result, tags = process_brand_content(content, api_key)
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '违规标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('brand', session_id, result, tags if tags else [])
                
                # 保存进度
                result_path = get_result_path('brand', session_id)
                df.to_excel(result_path, index=False)
                
                # 添加间隔
                time.sleep(0.5)
                
            except Exception as e:
                logger.error("品牌守护处理错误: %s" % str(e))
                update_task_status('brand', session_id, message='品牌内容 #%d 处理异常: %s，继续处理下一项' % (index+1, str(e)), status='warning')
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('brand', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('brand', session_id)
                df.to_excel(result_path, index=False)
                
                continue
        
        # 保存最终结果
        result_path = get_result_path('brand', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('brand', session_id, status='done', progress=100, message='品牌守护审核完成，请点击完成按钮')
        
        # 添加到历史记录
        add_to_history('brand', session_id, os.path.basename(filename), total_rows, 
                      task_status['brand'][session_id]['statistics'])
        
    except Exception as e:
        logger.error("品牌守护处理错误: %s" % str(e))
        update_task_status('brand', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也记录历史
        add_to_history('brand', session_id, os.path.basename(filename), total_rows, 
                      task_status['brand'][session_id]['statistics'])

def process_brand_content(content, api_key):
    """处理单条品牌内容"""
    max_retries = 3
    retry_count = 0
    api_timeout = (10, 3000)
    
    while retry_count < max_retries:
        try:
            # 构建请求数据
            data = {
                "query": "请审核以下内容是否存在品牌违规问题，并给出审核结果和违规标签：\n\n%s" % content,
                "inputs": {},
                "response_mode": "blocking",
                "user": "brand_audit_system"
            }
            
            # 发送请求
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % api_key
            }
            
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=api_timeout
            )
            
            if response.status_code != 200:
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    time.sleep(2)
                    continue
                response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 解析API返回结果
            result, tags = parse_audit_result(assistant_message)
            
            return result, tags
            
        except requests.exceptions.Timeout:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', []
            time.sleep(2 ** retry_count)
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', []
            time.sleep(2)
    
    return '处理失败', []
    
# ================ 资讯巡检功能保持不变 ================
def process_news_file(filename, combined_api_key, session_id):
    """处理资讯巡检文件 - 最终修复版（集成图片尺寸检查）"""
    try:
        # 解析API密钥
        api_keys = combined_api_key.split('|||')
        if len(api_keys) != 3:
            update_task_status('news', session_id, status='error', 
                             message='API密钥格式错误,需要3个密钥:信息读取|图片审核|文本审核')
            return
        
        api_key_info_extract = api_keys[0]  # 信息读取Agent
        api_key_image_audit = api_keys[1]   # 图片审核Agent
        api_key_text_audit = api_keys[2]    # 文本审核Agent
        
        # 读取Excel文件
        update_task_status('news', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')
        
        # 检查必要的列
        if '资讯链接' not in df.columns:
            update_task_status('news', session_id, status='error', 
                             message='文件格式错误：缺少"资讯链接"列')
            return
        
        # 数据清洗
        update_task_status('news', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['资讯链接'])
        df = df[df['资讯链接'].astype(str).str.strip() != '']
        
        # 初始化结果列
        df['审核结果'] = ''
        df['违规标签'] = ''
        df['提取图片数量'] = ''
        df['跳过小图片数量'] = ''  # 新增：记录跳过的图片数量
        df['图片审核详情'] = ''
        df['文本审核结果'] = ''
        df['文本违规标签'] = ''
        df['审核时间'] = ''
        
        total_rows = len(df)
        update_task_status('news', session_id, total=total_rows, 
                         message=f'数据准备完成，开始处理 {total_rows} 条资讯链接')
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查任务状态
                if task_status['news'][session_id]['status'] != 'processing':
                    break
                
                # 检查暂停状态
                while task_status['news'][session_id]['paused']:
                    time.sleep(0.5)
                    if task_status['news'][session_id]['status'] == 'idle':
                        return
                
                # 更新进度
                processed = index + 1
                progress = int((processed / total_rows) * 100)
                update_task_status('news', session_id, progress=progress, processed=processed, 
                                 message=f'开始处理资讯 #{index+1}/{total_rows}')
                
                # 处理资讯内容
                news_url = row['资讯链接']
                logger.info(f"处理资讯 #{index+1}: {news_url}")
                
                # 调用修复版处理函数（集成图片尺寸检查）
                result_data = process_news_item_complete(
                    news_url=news_url,
                    api_key_info_extract=api_key_info_extract,
                    api_key_image_audit=api_key_image_audit,
                    api_key_text_audit=api_key_text_audit,
                    session_id=session_id
                )
                
                # 更新结果
                df.at[index, '审核结果'] = result_data['final_result']
                df.at[index, '违规标签'] = ', '.join(result_data['final_tags'])
                df.at[index, '提取图片数量'] = result_data['image_count']
                df.at[index, '跳过小图片数量'] = result_data.get('skipped_small_images', 0)
                df.at[index, '图片审核详情'] = format_image_results(
                    result_data['image_results'], 
                    result_data.get('skipped_small_images', 0)
                 )
                df.at[index, '文本审核结果'] = result_data['text_result']
                df.at[index, '文本违规标签'] = ', '.join(result_data['text_tags'])
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                
                # 更新统计
                update_statistics('news', session_id, result_data['final_result'], 
                                result_data['final_tags'])
                
                # 实时保存进度
                result_path = get_result_path('news', session_id)
                df.to_excel(result_path, index=False)
                
                logger.info(f"资讯 #{index+1} 处理完成: {result_data['final_result']}")
                
            except Exception as e:
                logger.error(f"资讯处理错误 #{index+1}: {str(e)}")
                handle_processing_error(df, index, session_id, str(e))
                continue
        
        # 保存最终结果
        result_path = get_result_path('news', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('news', session_id, status='done', progress=100, 
                         message='资讯巡检完成，请点击完成按钮')
        
        # 添加到历史记录
        add_to_history('news', session_id, os.path.basename(filename), total_rows, 
                      task_status['news'][session_id]['statistics'])
        
        logger.info("资讯巡检任务完成")
        
    except Exception as e:
        logger.error(f"资讯处理错误: {str(e)}")
        update_task_status('news', session_id, status='error', 
                         message=f'处理出错: {str(e)}')
        # 即使出错也记录历史
        add_to_history('news', session_id, os.path.basename(filename), 
                      len(df) if 'df' in locals() else 0, 
                      task_status['news'][session_id]['statistics'])

def process_news_item_complete(news_url, api_key_info_extract, api_key_image_audit, 
                              api_key_text_audit, session_id):
    """处理单条资讯内容 - 最终修复版（集成图片尺寸检查）"""
    all_results = []
    all_tags = []
    image_count = 0
    text_audit_result = '未审核'
    text_audit_tags = []
    skipped_small_images = 0  # 新增：记录跳过的图片数量
    image_audit_details = []  # 新增：存储图片审核详情
    
    # 步骤1: 使用信息读取Agent同时提取图片链接和文本内容
    logger.info(f"开始提取资讯链接内容: {news_url}")
    
    update_task_status('news', session_id, message=f'正在读取资讯信息...')
    news_info = extract_news_info(news_url, api_key_info_extract)
    
    # 解析返回的信息
    image_urls = news_info.get('images', [])
    raw_text_content = news_info.get('content', '')
    
    logger.info(f"提取到 {len(image_urls)} 张图片")
    logger.info(f"提取到原始文本内容长度: {len(raw_text_content)}")
    
    # 步骤2: 截取有效文本内容
    text_content = extract_valid_content(raw_text_content)
    logger.info(f"截取后文本内容长度: {len(text_content)}")
    
    # 步骤3: 审核所有图片（增加尺寸检查）
    if image_urls:
        logger.info(f"开始审核 {len(image_urls)} 张图片内容")
        for i, image_url in enumerate(image_urls):
            try:
                update_task_status('news', session_id, message=f'正在检查图片 {i+1}/{len(image_urls)} 尺寸...')
                
                # 新增：检查图片尺寸
                should_audit, size_info = check_image_size(image_url)
                
                if not should_audit:
                    # 图片尺寸过小，跳过审核
                    logger.info(f"图片 {i+1} 尺寸过小，跳过审核: {size_info}")
                    skipped_small_images += 1
                    continue  # 直接跳过，不记录任何结果
                
                update_task_status('news', session_id, message=f'正在审核图片 {i+1}/{len(image_urls)}...')
                
                # 修复：处理URL编码问题
                clean_image_url = clean_image_url_for_api(image_url)
                result, tags = audit_news_image_fixed(clean_image_url, api_key_image_audit)
                
                # 只记录违规图片的结果
                if result == '违规':
                    all_results.append(result)
                    all_tags.extend(tags)
                    # 记录图片审核详情
                    image_audit_details.append({
                        'index': i+1,
                        'result': result,
                        'tags': tags
                    })
                
                logger.info(f"图片 {i+1}/{len(image_urls)} 审核完成: {result}, 标签: {tags}")
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"图片 {i+1} 处理失败: {str(e)}")
                # 处理失败的图片也记录下来
                all_results.append('处理失败')
                all_tags.append('图片处理失败')
                continue
    else:
        logger.warning("没有提取到图片链接")
        all_results.append('无图片')
        all_tags.append('无图片')
    
    # 步骤4: 审核文本内容
    if text_content and text_content != "文本提取失败":
        try:
            update_task_status('news', session_id, message='正在审核文本内容...')
            text_audit_result, text_audit_tags = audit_news_text_fixed(text_content, api_key_text_audit)
            all_results.append(text_audit_result)
            all_tags.extend(text_audit_tags)
            logger.info(f"文本审核完成: {text_audit_result}, 标签: {text_audit_tags}")
        except Exception as e:
            logger.error(f"文本审核失败: {str(e)}")
            all_results.append('处理失败')
            all_tags.append('文本审核失败')
    else:
        logger.warning("文本内容提取失败，跳过文本审核")
        all_results.append('文本提取失败')
        all_tags.append('文本提取失败')
    
    # 汇总结果
    logger.info(f"所有审核结果: {all_results}")
    logger.info(f"所有审核标签: {all_tags}")
    logger.info(f"跳过的图片数量: {skipped_small_images}")
    
    final_result, final_tags = aggregate_news_results(all_results, all_tags)
    
    logger.info(f"资讯审核完成: 结果={final_result}, 标签={final_tags}, 总图片={len(image_urls)}, 跳过={skipped_small_images}, 文本结果={text_audit_result}")
    
    return {
        'final_result': final_result,
        'final_tags': final_tags,
        'image_count': len(image_urls),
        'skipped_small_images': skipped_small_images,  # 新增：返回跳过的图片数量
        'image_results': image_audit_details,  # 修改：只包含违规图片的详情
        'text_result': text_audit_result,
        'text_tags': text_audit_tags
    }

def check_image_size(image_url):
    """检查图片尺寸，如果长或宽小于600像素则跳过审核"""
    try:
        # 发送HEAD请求获取图片信息，避免下载整个图片
        response = requests.head(image_url, timeout=5)
        
        # 检查是否支持获取尺寸信息
        if 'content-length' in response.headers:
            content_length = int(response.headers.get('content-length', 0))
            # 如果图片很小，直接判断为小图片
            if content_length < 1024:  # 小于1KB的图片很可能是小图片
                return False, "图片尺寸过小"
        
        # 对于不支持HEAD请求或需要实际下载的情况，使用GET请求但限制数据量
        response = requests.get(image_url, stream=True, timeout=10)
        response.raise_for_status()
        
        # 只读取前1MB数据来检查图片尺寸
        img_data = b""
        for chunk in response.iter_content(chunk_size=8192):
            img_data += chunk
            if len(img_data) > 1024 * 1024:  # 最多读取1MB
                break
        
        # 使用PIL检查图片尺寸
        try:
            from PIL import Image
            import io
            
            img = Image.open(io.BytesIO(img_data))
            width, height = img.size
            
            logger.info(f"图片尺寸检查: {image_url} - {width}x{height}")
            
            # 如果长或宽小于200像素，返回False
            if width < 600 or height < 600:
                return False, f"图片尺寸过小({width}x{height})"
            else:
                return True, f"图片尺寸合格({width}x{height})"
                
        except ImportError:
            # 如果PIL不可用，默认继续审核
            logger.warning("PIL库未安装，无法检查图片尺寸，继续审核")
            return True, "PIL不可用，继续审核"
            
    except Exception as e:
        logger.warning(f"图片尺寸检查失败 {image_url}: {str(e)}")
        # 如果检查失败，默认继续审核（保守策略）
        return True, f"尺寸检查失败，继续审核: {str(e)}"

def extract_news_info(news_url, api_key):
    """信息读取Agent - 同时提取图片链接和文本内容"""
    max_retries = 3
    retry_count = 0
    api_timeout = (10, 30000)
    
    while retry_count < max_retries:
        try:
            data = {
                "query": f"{news_url}",
                "inputs": {},
                "response_mode": "blocking",
                "user": "news_info_extractor"
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            logger.info(f"发送信息读取请求: {news_url}")
            response = requests.post(
                API_URL,
                headers=headers,
                json=data,
                timeout=api_timeout
            )
            
            if response.status_code != 200:
                logger.error(f"信息读取响应错误: {response.status_code} - {response.text}")
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    time.sleep(2)
                    continue
                response.raise_for_status()
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            logger.info(f"信息读取原始响应: {assistant_message[:500]}...")
            
            # 解析返回的信息
            news_info = parse_news_info(assistant_message)
            logger.info(f"解析到图片数量: {len(news_info.get('images', []))}, 文本长度: {len(news_info.get('content', ''))}")
            return news_info
            
        except Exception as e:
            retry_count += 1
            logger.error(f"提取资讯信息失败 (尝试 {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                return {'images': [], 'content': '信息提取失败'}
            time.sleep(2)
    
    return {'images': [], 'content': '信息提取失败'}

def parse_news_info(message):
    """解析信息读取Agent返回的内容"""
    try:
        # 移除think标签
        think_pattern = r'<think>.*?</think>'
        cleaned_message = re.sub(think_pattern, '', message, flags=re.DOTALL)
        
        # 尝试直接解析JSON格式
        import json
        # 查找大括号包裹的内容
        json_match = re.search(r'\{[^}]*["\']images["\'][^}]*["\']content["\'][^}]*\}', cleaned_message, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            # 尝试解析JSON
            try:
                parsed_data = json.loads(json_str)
                images = parsed_data.get('images', [])
                content = parsed_data.get('content', '')
                
                # 确保 images 是列表
                if isinstance(images, str):
                    # 如果是字符串，按逗号分割
                    images = [img.strip() for img in images.split(',') if img.strip()]
                
                return {'images': images, 'content': content}
            except:
                pass
        
        # 如果JSON解析失败，使用正则表达式提取
        images = []
        content = ''
        
        # 提取images字段
        images_match = re.search(r'["\']images["\']\s*:\s*\[([^\]]+)\]', cleaned_message)
        if images_match:
            images_str = images_match.group(1)
            # 提取所有图片URL
            image_urls = re.findall(r'["\']([^"\',]+)["\']', images_str)
            images = [url.strip() for url in image_urls if url.strip().startswith('http')]
        
        # 如果没有找到images字段，直接提取URL
        if not images:
            url_patterns = [
                r'https?://[^\s<>"\)\]]+',  # 基础URL模式
            ]
            for pattern in url_patterns:
                matches = re.findall(pattern, cleaned_message, re.IGNORECASE)
                if matches:
                    images = [url for url in matches if any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', 'image'])]
                    if images:
                        break
        
        # 提取content字段
        content_match = re.search(r'["\']content["\']\s*:\s*["\']([^"\']*)["\']', cleaned_message, re.DOTALL)
        if content_match:
            content = content_match.group(1)
        else:
            # 如果没有找到content字段，尝试提取整个文本
            # 移除图片URL后的内容作为文本
            content = cleaned_message
            for img_url in images:
                content = content.replace(img_url, '')
            content = re.sub(r'["\']images["\']\s*:\s*\[[^\]]+\]', '', content)
            content = content.strip()
        
        logger.info(f"解析结果: 图片数量={len(images)}, 文本长度={len(content)}")
        return {'images': images, 'content': content}
        
    except Exception as e:
        logger.error(f"解析信息失败: {str(e)}")
        return {'images': [], 'content': message}

def extract_valid_content(raw_content):
    """从原始文本中截取有效内容"""
    if not raw_content:
        return "文本提取失败"
    
    # 查找截止关键词
    stop_keywords = ['精彩推荐', '相关推荐', '全部评论', '我要举报']
    
    # 找到最早出现的截止关键词
    earliest_pos = len(raw_content)
    found_keyword = None
    
    for keyword in stop_keywords:
        pos = raw_content.find(keyword)
        if pos != -1 and pos < earliest_pos:
            earliest_pos = pos
            found_keyword = keyword
    
    # 截取内容
    if found_keyword:
        valid_content = raw_content[:earliest_pos].strip()
        logger.info(f"在位置 {earliest_pos} 找到截止关键词 '{found_keyword}'，截取后内容长度: {len(valid_content)}")
    else:
        valid_content = raw_content.strip()
        logger.info(f"未找到截止关键词，使用全部内容，长度: {len(valid_content)}")
    
    # 验证文本质量
    if len(valid_content) < 20:
        return "文本提取失败"
    
    return valid_content

def clean_image_url_for_api(image_url):
    """清理图片URL，处理特殊字符问题"""
    try:
        # 对URL进行编码处理，但保留协议和域名部分
        from urllib import quote
        
        # 分离协议和路径
        if '://' in image_url:
            protocol, path = image_url.split('://', 1)
            # 对路径部分进行编码
            encoded_path = quote(path, safe='/:?=&')
            return f"{protocol}://{encoded_path}"
        else:
            # 如果没有协议，直接编码
            return quote(image_url, safe='/:?=&')
    except:
        # 如果编码失败，返回原始URL
        return image_url

def audit_news_image_fixed(image_url, api_key):
    """审核单张图片 - 修复版（处理URL编码问题）"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 构建请求数据 - 使用修复的URL
            data = {
                "query": "请审核以下图片内容是否违规，并给出审核结果和违规标签：",
                "inputs": {},
                "response_mode": "blocking",
                "user": "news_image_auditor",
                "upload_mediums": [
                    {
                        "url": image_url,
                        "type": "image"
                    }
                ]
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            logger.info(f"发送图片审核请求: {image_url[:100]}...")  # 只记录前100个字符
            
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=(10, 3000)
            )
            
            logger.info(f"图片审核响应状态: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"图片审核响应错误: {response.text}")
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    logger.warning(f"501错误触发重试 ({retry_count}/{max_retries})")
                    time.sleep(2)
                    continue
                response.raise_for_status()
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            logger.info(f"图片审核原始响应: {assistant_message}")
            
            # 使用统一的审核结果解析
            result, tags = parse_audit_result(assistant_message)
            logger.info(f"图片审核解析结果: {result}, 标签: {tags}")
            return result, tags
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            logger.error(f"网络请求异常 (尝试 {retry_count}/{max_retries}): {str(req_err)}")
            
            if retry_count >= max_retries:
                return '处理失败', ['图片审核失败']
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
            logger.error(f"未处理的异常 (尝试 {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                return '处理失败', ['图片审核失败']
            time.sleep(2)
    
    return '处理失败', ['图片审核失败']

def audit_news_text_fixed(text_content, api_key):
    """审核文本内容 - 修复版"""
    max_retries = 3
    retry_count = 0
    api_timeout = (10, 3000)
    
    while retry_count < max_retries:
        try:
            # 限制文本长度
            if len(text_content) > 3000:
                text_content = text_content[:3000] + "..."
            
            data = {
                "query": f"请审核以下资讯文本内容是否违规，并给出审核结果和违规标签：\n\n{text_content}",
                "inputs": {},
                "response_mode": "blocking",
                "user": "news_text_auditor"
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            logger.info(f"发送文本审核请求，文本长度: {len(text_content)}")
            response = requests.post(
                API_URL,
                headers=headers,
                json=data,
                timeout=api_timeout
            )
            
            if response.status_code != 200:
                logger.error(f"文本审核响应错误: {response.status_code} - {response.text}")
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    time.sleep(2)
                    continue
                response.raise_for_status()
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 使用统一的审核结果解析
            result, tags = parse_audit_result(assistant_message)
            logger.info(f"文本审核解析结果: {result}, 标签: {tags}")
            return result, tags
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            logger.error(f"网络请求异常 (尝试 {retry_count}/{max_retries}): {str(req_err)}")
            
            if retry_count >= max_retries:
                return '处理失败', ['文本审核失败']
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
            logger.error(f"未处理的异常 (尝试 {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                return '处理失败', ['文本审核失败']
            time.sleep(2)
    
    return '处理失败', ['文本审核失败']

def aggregate_news_results(all_results, all_tags):
    """汇总审核结果 - 过滤掉小图片标签"""
    logger.info(f"所有审核结果: {all_results}")
    logger.info(f"所有审核标签: {all_tags}")
    
    # 过滤掉小图片相关的标签
    filtered_tags = [tag for tag in all_tags if tag != '小图片']
    
    # 判断最终结果
    if '违规' in all_results:
        final_result = '违规'
    elif '处理失败' in all_results:
        final_result = '处理失败'
    elif all(r in ['无图片', '文本提取失败'] for r in all_results):
        final_result = '无内容'
    else:
        final_result = '正常'
    
    # 处理标签，过滤掉空标签和无效标签
    final_tags = list(set([tag for tag in filtered_tags if tag and tag not in ['/', '无标签']]))
    if not final_tags:
        final_tags = ['/']
    
    logger.info(f"过滤后的标签: {final_tags}")
    return final_result, final_tags

def format_image_results(image_results, skipped_count=0):
    """格式化图片审核结果 - 只展示违规图片"""
    if not image_results:
        if skipped_count > 0:
            return f"所有图片均为小图片或正常图片（跳过{skipped_count}张小图片）"
        else:
            return "无图片或所有图片均正常"
    
    details = []
    
    for result in image_results:
        # 只展示违规图片
        if result['result'] == '违规':
            tags_str = ','.join(result['tags']) if result['tags'] else '无标签'
            details.append(f"图片{result['index']}:违规({tags_str})")
    
    # 如果没有违规图片
    if not details:
        if skipped_count > 0:
            return f"所有图片均为小图片或正常图片（跳过{skipped_count}张小图片）"
        else:
            return "所有图片均正常"
    
    return "; ".join(details)

def handle_processing_error(df, index, session_id, error_msg):
    """处理处理过程中的错误"""
    update_task_status('news', session_id, 
                     message=f'资讯 #{index+1} 处理异常: {error_msg}，继续处理下一项', 
                     status='warning')
    
    # 更新结果为处理失败
    df.at[index, '审核结果'] = '处理失败'
    df.at[index, '违规标签'] = '/'
    df.at[index, '提取图片数量'] = 0
    df.at[index, '跳过小图片数量'] = 0
    df.at[index, '图片审核详情'] = '处理失败'
    df.at[index, '文本审核结果'] = '处理失败'
    df.at[index, '文本违规标签'] = '/'
    df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 更新统计
    update_statistics('news', session_id, '处理失败', [])
    
    # 保存当前结果
    result_path = get_result_path('news', session_id)
    df.to_excel(result_path, index=False)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)