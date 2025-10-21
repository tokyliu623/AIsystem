# 完整的 app.py 代码文件 - 日志优化版
"""
AI智能内容巡检系统 - 修复版（日志优化）

支持评论审核、封面审核、智慧push审核、品牌守护审核和资讯巡检功能

主应用模块，处理Web请求和任务管理

优化内容：
1. 统一日志输出格式，减少冗余日志
2. 优化不同类型巡检的过程输出，保持一致性
3. 降低日志输出频率，提升系统性能
4. 保留关键错误和状态变更日志
"""

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

MAX_RETRIES = 3  # 最大重试次数
API_TIMEOUT = 3000  # API超时时间（秒）
TIMEOUT = 3000 

# 在文件顶部添加常量
HISTORY_DIR = 'history'
HISTORY_INDEX_FILE = os.path.join(HISTORY_DIR, 'index.json')
os.makedirs(HISTORY_DIR, exist_ok=True)

# 配置日志 - 优化输出级别
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

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
    'push': {},  # 新增智慧push审核类型
    'brand': {},  # 新增品牌守护审核类型
    'news': {}    # 新增资讯巡检类型
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
            
        logger.info(f"[历史记录] {audit_type}任务完成 - 文件: {filename}, 总数: {total_rows}")
        return True
    except Exception as e:
        logger.error(f"[历史记录] 保存失败: {str(e)}")
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
        logger.error(f"[历史记录] 分页查询错误: {str(e)}")
        return jsonify({'error': '获取历史记录失败: %s' % str(e)}), 500

# 修复历史统计API - 添加巡检量级统计并支持时间筛选
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
                'by_volume': {}  # 新增巡检量级统计
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
            'by_volume': by_volume  # 返回巡检量级统计
        })
        
    except Exception as e:
        logger.error(f"[历史记录] 统计查询错误: {str(e)}")
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
        logger.error(f"[历史记录] 导出错误: {str(e)}")
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
        
        # 修复审核类型验证
        valid_audit_types = ['comment', 'cover', 'push', 'brand', 'news']  # 添加news类型
        if audit_type not in valid_audit_types:
            return jsonify({
                'error': '无效的审核类型: %s' % audit_type,
                'valid_types': valid_audit_types
            }), 400
        
        # 确保push、brand和news类型有完整的任务状态结构
        if audit_type == 'push':
            if session_id not in task_status['push']:
                task_status['push'][session_id] = {
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
                    'history': [],
                    'conversation_id': ''  # push审核专用字段
                }
        elif audit_type == 'brand':
            if session_id not in task_status['brand']:
                task_status['brand'][session_id] = {
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
        elif audit_type == 'news':  # 新增news类型的初始化
            if session_id not in task_status['news']:
                task_status['news'][session_id] = {
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
        
        logger.info(f"[{audit_type.upper()}] 文件上传成功: {file.filename}")
        return jsonify({'message': '文件上传成功'})
        
    except Exception as e:
        logger.error(f"[{audit_type.upper()}] 文件上传错误: {str(e)}")
        return jsonify({'error': '文件上传失败: %s' % str(e)}), 500

@app.route('/run', methods=['POST'])
def run_task():
    """启动任务"""
    try:
        # 获取参数
        data = request.get_json()
        audit_type = data.get('audit_type')
        api_key = data.get('api_key')
        session_id = data.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:  # 添加news类型
            return jsonify({'error': '无效的审核类型'}), 400
        
        if not api_key:
            return jsonify({'error': 'API密钥不能为空'}), 400
        
        # 检查文件是否存在
        filename = get_upload_path(audit_type, session_id)
        if not os.path.exists(filename):
            return jsonify({'error': '请先上传文件'}), 400
        
        # 更新任务状态
        update_task_status(audit_type, session_id, status='processing', progress=0, message='开始处理...')
        
        # 启动处理线程
        if audit_type == 'comment':
            thread = threading.Thread(target=process_comment_file, args=(filename, api_key, session_id))
        elif audit_type == 'cover':
            thread = threading.Thread(target=process_cover_file, args=(filename, api_key, session_id))
        elif audit_type == 'push':
            thread = threading.Thread(target=process_push_file, args=(filename, api_key, session_id))
        elif audit_type == 'brand':
            thread = threading.Thread(target=process_brand_file, args=(filename, api_key, session_id))
        elif audit_type == 'news':
            # 对于资讯巡检，api_key 包含两个密钥，用分隔符连接
            thread = threading.Thread(target=process_news_file, args=(filename, api_key, session_id))
        
        thread.daemon = True
        thread.start()
        
        logger.info(f"[{audit_type.upper()}] 任务启动 - 会话: {session_id}")
        return jsonify({'message': '任务已启动'})
        
    except Exception as e:
        logger.error(f"[{audit_type.upper()}] 启动任务错误: {str(e)}")
        return jsonify({'error': '启动任务失败: %s' % str(e)}), 500

@app.route('/status/<audit_type>')
def get_status(audit_type):
    """获取任务状态"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:  # 添加news类型
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
        logger.error(f"[{audit_type.upper()}] 获取状态错误: {str(e)}")
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
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:  # 添加news类型
            return jsonify({'error': '无效的审核类型'}), 400
        
        if action not in ['pause', 'resume', 'finish', 'end']:
            return jsonify({'error': '无效的操作'}), 400
        
        if session_id not in task_status[audit_type]:
            return jsonify({'error': '任务不存在'}), 404
        
        # 执行操作
        if action == 'pause':
            task_status[audit_type][session_id]['paused'] = True
            update_task_status(audit_type, session_id, message='任务已暂停')
            logger.info(f"[{audit_type.upper()}] 任务暂停 - 会话: {session_id}")
            return jsonify({'message': '任务已暂停'})
            
        elif action == 'resume':
            task_status[audit_type][session_id]['paused'] = False
            update_task_status(audit_type, session_id, message='任务已继续')
            logger.info(f"[{audit_type.upper()}] 任务继续 - 会话: {session_id}")
            return jsonify({'message': '任务已继续'})
            
        elif action == 'finish':
            update_task_status(audit_type, session_id, status='done', message='任务已完成')
            logger.info(f"[{audit_type.upper()}] 任务完成 - 会话: {session_id}")
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
            logger.info(f"[{audit_type.upper()}] 任务结束 - 会话: {session_id}")
            return jsonify({'message': '任务已结束'})
        
    except Exception as e:
        logger.error(f"[{audit_type.upper()}] 控制任务错误: {str(e)}")
        return jsonify({'error': '控制任务失败: %s' % str(e)}), 500

@app.route('/statistics/<audit_type>')
def get_statistics(audit_type):
    """获取统计数据"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:  # 添加news类型
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
        logger.error(f"[{audit_type.upper()}] 获取统计数据错误: {str(e)}")
        return jsonify({'error': '获取统计数据失败: %s' % str(e)}), 500

@app.route('/download/<audit_type>')
def download_result(audit_type):
    """下载结果文件"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover', 'push', 'brand', 'news']:  # 添加news类型
            return jsonify({'error': '无效的审核类型'}), 400
        
        # 获取结果文件路径
        result_path = get_result_path(audit_type, session_id)
        
        if not os.path.exists(result_path):
            return jsonify({'error': '结果文件不存在'}), 404
        
        # 返回文件
        logger.info(f"[{audit_type.upper()}] 下载结果文件 - 会话: {session_id}")
        return send_file(result_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"[{audit_type.upper()}] 下载结果错误: {str(e)}")
        return jsonify({'error': '下载结果失败: %s' % str(e)}), 500

@app.route('/history')
def get_history():
    """获取历史记录"""
    try:
        return jsonify({'history': history_records})
        
    except Exception as e:
        logger.error(f"[历史记录] 获取错误: {str(e)}")
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
        logger.info(f"[历史记录] 下载历史文件: {history_id}")
        return send_file(result_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"[历史记录] 下载历史结果错误: {str(e)}")
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

        logger.info(f"[历史记录] 删除记录: {history_id}")
        return jsonify({"message": "历史记录删除成功", "id": history_id}), 200

    except Exception as e:
        logger.error(f"[历史记录] 删除失败: {str(e)}")
        return jsonify({"error": f"删除失败: {e}"}), 500

def process_comment_file(filename, api_key, session_id):
    """处理评论文件 - 使用传入的session_id而非Flask session"""
    try:
        # 读取Excel文件
        update_task_status('comment', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')# 直接指定使用openpyxl引擎
        
        # 检查必要的列
        if '评论内容' not in df.columns:
            update_task_status('comment', session_id, status='error', message='文件格式错误：缺少"评论内容"列')
            return
        
        # 数据清洗
        update_task_status('comment', session_id, message='开始数据清洗...')
        df = df.dropna(subset=['评论内容'])
        df = df[df['评论内容'].astype(str).str.strip() != '']
        
        # 初始化结果列
        df['审核结果'] = ''
        df['违规标签'] = ''
        df['审核时间'] = ''
        
        total_rows = len(df)
        update_task_status('comment', session_id, total=total_rows, message='数据准备完成，开始处理 %d 条评论' % total_rows)
        
        logger.info(f"[COMMENT] 开始处理评论文件 - 总数: {total_rows}")
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查是否暂停
                while task_status['comment'][session_id]['paused']:
                    time.sleep(0.5)
                    # 检查是否已结束任务
                    if task_status['comment'][session_id]['status'] == 'idle':
                        return
                
                # 检查任务状态
                if task_status['comment'][session_id]['status'] != 'processing':
                    break
                
                # 更新进度
                processed = index + 1
                progress = int((processed / total_rows) * 100)
                
                # 每10%记录一次进度日志
                if processed == 1 or processed % max(1, total_rows // 10) == 0 or processed == total_rows:
                    logger.info(f"[COMMENT] 处理进度: {processed}/{total_rows} ({progress}%)")
                    update_task_status('comment', session_id, progress=progress, processed=processed, 
                                     message='处理中: %d/%d (%d%%)' % (processed, total_rows, progress))
                else:
                    update_task_status('comment', session_id, progress=progress, processed=processed)
                
                # 处理评论
                comment = row['评论内容']
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
                
                # 每10条或最后一条保存一次结果
                if (index + 1) % 10 == 0 or (index + 1) == total_rows:
                    result_path = get_result_path('comment', session_id)
                    df.to_excel(result_path, index=False)
                
            except Exception as e:
                logger.error(f"[COMMENT] 处理项目 #{index+1} 错误: {str(e)}")
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('comment', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('comment', session_id)
                df.to_excel(result_path, index=False)
        
        # 保存最终结果
        result_path = get_result_path('comment', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('comment', session_id, status='done', progress=100, message='评论审核完成')
        
        # 添加到历史记录      
        add_to_history('comment', session_id, os.path.basename(filename), total_rows, 
                      task_status['comment'][session_id]['statistics'])
        
        logger.info(f"[COMMENT] 任务完成 - 总数: {total_rows}")
        
    except Exception as e:
        logger.error(f"[COMMENT] 处理错误: {str(e)}")
        update_task_status('comment', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也记录历史
        add_to_history('comment', session_id, os.path.basename(filename), total_rows, 
                      task_status['comment'][session_id]['statistics'])   

def process_cover_file(filename, api_key, session_id):
    """处理封面文件 - 使用传入的session_id而非Flask session"""
    try:
        # 读取Excel文件
        update_task_status('cover', session_id, message='读取文件中...')
        df = pd.read_excel(filename, engine='openpyxl')# 直接指定使用openpyxl引擎
        
        
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
        
        logger.info(f"[COVER] 开始处理封面文件 - 总数: {total_rows}")
        
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
                
                # 每10%记录一次进度日志
                if processed == 1 or processed % max(1, total_rows // 10) == 0 or processed == total_rows:
                    logger.info(f"[COVER] 处理进度: {processed}/{total_rows} ({progress}%)")
                    update_task_status('cover', session_id, progress=progress, processed=processed, 
                                     message='处理中: %d/%d (%d%%)' % (processed, total_rows, progress))
                else:
                    update_task_status('cover', session_id, progress=progress, processed=processed)
                
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
                
                # 每10条或最后一条保存一次结果
                if (index + 1) % 10 == 0 or (index + 1) == total_rows:
                    result_path = get_result_path('cover', session_id)
                    df.to_excel(result_path, index=False)
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"[COVER] 处理项目 #{index+1} 错误: {str(e)}")
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('cover', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('cover', session_id)
                df.to_excel(result_path, index=False)
        
        # 保存最终结果
        result_path = get_result_path('cover', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('cover', session_id, status='done', progress=100, message='封面审核完成')
      
        # 在函数末尾，修改 add_to_history 调用
        add_to_history('cover', session_id, os.path.basename(filename), total_rows, 
                      task_status['cover'][session_id]['statistics'])
        
        logger.info(f"[COVER] 任务完成 - 总数: {total_rows}")
        
    except Exception as e:
        logger.error(f"[COVER] 处理错误: {str(e)}")
        update_task_status('cover', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也记录历史
        add_to_history('cover', session_id, os.path.basename(filename), total_rows, 
                      task_status['cover'][session_id]['statistics'])

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
            
            # 发送请求，添加3000秒超时机制
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=api_timeout
            )
            
            # 处理非200状态码
            if response.status_code != 200:
                # 特殊处理501错误
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    time.sleep(2)
                    continue
                # 其他非200状态码直接引发异常
                response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 解析API返回结果
            result, tags = parse_audit_result(assistant_message)
            
            return result, tags
            
        except requests.exceptions.Timeout as timeout_err:
            # 专门处理超时异常
            retry_count += 1
            timeout_type = "连接" if "connect" in str(timeout_err).lower() else "读取"
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            # 指数退避策略
            sleep_time = 2 ** retry_count
            time.sleep(sleep_time)
            
        except requests.exceptions.RequestException as req_err:
            # 处理其他网络请求异常
            retry_count += 1
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
            
        except Exception as e:
            # 处理其他未预期的异常
            retry_count += 1
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
    
    return '处理失败', []

def parse_audit_result(assistant_message):
    """解析审核结果 - 修复版本，支持多种格式和增强错误处理"""
    result = "解析失败"
    tags = []
    
    try:
        # 第一步：过滤think标签内容
        think_pattern = r'<think>.*?</think>'
        filtered_message = re.sub(think_pattern, '', assistant_message, flags=re.DOTALL).strip()
        
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
            
            if not tag_found:
                tag_match = re.search(tag_pattern, filtered_message, re.IGNORECASE)
                if tag_match:
                    tag_str = tag_match.group(1).strip()
                    tags = parse_tags(tag_str)
                    tag_found = True
        
        # 第三步：备用解析策略 - 基于关键词匹配
        if not result_found:
            if '正常' in filtered_message and ('违规' not in filtered_message and '低质' not in filtered_message):
                result = '正常'
                result_found = True
            elif '低质' in filtered_message or '违规' in filtered_message:
                result = '低质'
                result_found = True
        
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
        
    except Exception as e:
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
 
def process_cover(cover_url, api_key, index, session_id):
    """处理单条封面链接 - 适配新的API接口"""
    # 应用速率限制
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
            
            # 发送请求
            response = requests.post(API_URL, headers=headers, json=data)
            
            # 处理非200状态码
            if response.status_code != 200:
                # 特殊处理501错误（对话ID不存在）
                if response.status_code == 501 and "conversation_id" in response.text:
                    # 重新尝试，不使用conversation_id
                    retry_count += 1
                    time.sleep(2)  # 等待2秒后重试
                    continue
            
            response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 保存conversation_id以便后续使用
            conversation_id = result_data.get('conversation_id', '')
            
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
            
            return result, tags
            
        except requests.exceptions.Timeout as timeout_err:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', []
            
            sleep_time = 2 ** retry_count
            time.sleep(sleep_time)
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
    
    return '处理失败', []

# ================ 修复的智慧Push审核功能 ================

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
                break
        
        # 4. 备用策略：关键词匹配
        if result == "解析失败":
            if '正常' in filtered_message and '违规' not in filtered_message and '低质' not in filtered_message:
                result = '正常'
            elif '低质' in filtered_message or '违规' in filtered_message:
                result = '低质'
        
        # 5. 后处理
        if result == '正常':
            tags = []  # 正常内容不应有标签
            
        return result, tags
        
    except Exception as e:
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

def audit_content(title, summary, api_key, session_id):
    """执行双字段内容审核（增强错误处理）"""
    # 从任务状态中获取会话ID
    if session_id in task_status['push']:
        conversation_id = task_status['push'][session_id].get('conversation_id', '')
    else:
        conversation_id = ''
    
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
        # 更新会话ID
        if session_id in task_status['push']:
            task_status['push'][session_id]['conversation_id'] = response_data.get("conversation_id", '')
        answer = response_data.get("answer", "")
        
        return parse_audit_result_push(answer)
        
    except requests.exceptions.Timeout:
        return ("请求超时", [])
    except Exception as e:
        return ("服务异常", [])

def process_push_file(filename, api_key, session_id):
    """处理智慧Push文件"""
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
        
        # 初始化结果列
        df['审核结果'] = ''
        df['低质标签'] = ''
        df['审核时间'] = ''
        
        total_rows = len(df)
        update_task_status('push', session_id, total=total_rows, message='数据准备完成，开始处理 %d 条Push内容' % total_rows)
        
        logger.info(f"[PUSH] 开始处理Push文件 - 总数: {total_rows}")
        
        # 初始化会话ID
        if session_id not in task_status['push']:
            task_status['push'][session_id] = {'conversation_id': ''}
        else:
            task_status['push'][session_id]['conversation'] = ''
        
        # 逐行处理数据
        for index, row in df.iterrows():
            try:
                # 检查是否暂停
                while task_status['push'][session_id]['paused']:
                    time.sleep(0.5)
                    if task_status['push'][session_id]['status'] == 'idle':
                        return
                
                # 检查任务状态
                if task_status['push'][session_id]['status'] != 'processing':
                    break
                
                # 更新进度
                processed = index + 1
                progress = int((processed / total_rows) * 100)
                
                # 每10%记录一次进度日志
                if processed == 1 or processed % max(1, total_rows // 10) == 0 or processed == total_rows:
                    logger.info(f"[PUSH] 处理进度: {processed}/{total_rows} ({progress}%)")
                    update_task_status('push', session_id, progress=progress, processed=processed, 
                                     message='处理中: %d/%d (%d%%)' % (processed, total_rows, progress))
                else:
                    update_task_status('push', session_id, progress=progress, processed=processed)
                
                # 处理内容
                title = str(row['标题']).strip()
                summary = str(row['摘要']).strip()
                result, tags = audit_content(title, summary, api_key, session_id)
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '低质标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('push', session_id, result, tags if tags else [])
                
                # 每10条或最后一条保存一次结果
                if (index + 1) % 10 == 0 or (index + 1) == total_rows:
                    result_path = get_result_path('push', session_id)
                    df.to_excel(result_path, index=False)
                
                # 添加间隔
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[PUSH] 处理项目 #{index+1} 错误: {str(e)}")
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '低质标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('push', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('push', session_id)
                df.to_excel(result_path, index=False)
        
        # 保存最终结果
        result_path = get_result_path('push', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('push', session_id, status='done', progress=100, message='智慧Push审核完成')
        
        # 添加到历史记录
        add_to_history('push', session_id, os.path.basename(filename), total_rows, 
                      task_status['push'][session_id]['statistics'])
        
        logger.info(f"[PUSH] 任务完成 - 总数: {total_rows}")
        
    except Exception as e:
        logger.error(f"[PUSH] 处理错误: {str(e)}")
        update_task_status('push', session_id, status='error', message='处理出错: %s' % str(e))
        # 即使出错也记录历史
        add_to_history('push', session_id, os.path.basename(filename), total_rows, 
                      task_status['push'][session_id]['statistics'])

# ================ 品牌守护审核功能 ================

def process_brand_file(filename, api_key, session_id):
    """处理品牌守护文件"""
    try:
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
        
        logger.info(f"[BRAND] 开始处理品牌文件 - 总数: {total_rows}")
        
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
                
                # 每10%记录一次进度日志
                if processed == 1 or processed % max(1, total_rows // 10) == 0 or processed == total_rows:
                    logger.info(f"[BRAND] 处理进度: {processed}/{total_rows} ({progress}%)")
                    update_task_status('brand', session_id, progress=progress, processed=processed, 
                                     message='处理中: %d/%d (%d%%)' % (processed, total_rows, progress))
                else:
                    update_task_status('brand', session_id, progress=progress, processed=processed)
                
                # 处理内容
                content = row['品牌标题']
                result, tags = process_brand_content(content, api_key)
                
                # 更新结果
                df.at[index, '审核结果'] = result
                df.at[index, '违规标签'] = ', '.join(tags) if tags else '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('brand', session_id, result, tags if tags else [])
                
                # 每10条或最后一条保存一次结果
                if (index + 1) % 10 == 0 or (index + 1) == total_rows:
                    result_path = get_result_path('brand', session_id)
                    df.to_excel(result_path, index=False)
                
                # 添加间隔
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[BRAND] 处理项目 #{index+1} 错误: {str(e)}")
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('brand', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('brand', session_id)
                df.to_excel(result_path, index=False)
        
        # 保存最终结果
        result_path = get_result_path('brand', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('brand', session_id, status='done', progress=100, message='品牌守护审核完成')
        
        # 添加到历史记录
        add_to_history('brand', session_id, os.path.basename(filename), total_rows, 
                      task_status['brand'][session_id]['statistics'])
        
        logger.info(f"[BRAND] 任务完成 - 总数: {total_rows}")
        
    except Exception as e:
        logger.error(f"[BRAND] 处理错误: {str(e)}")
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

# ================ 新增图片尺寸检查功能 ================

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
            
            # 如果长或宽小于200像素，返回False
            if width < 600 or height < 600:
                return False, f"图片尺寸过小({width}x{height})"
            else:
                return True, f"图片尺寸合格({width}x{height})"
                
        except ImportError:
            # 如果PIL不可用，默认继续审核
            return True, "PIL不可用，继续审核"
            
    except Exception as e:
        # 如果检查失败，默认继续审核（保守策略）
        return True, f"尺寸检查失败，继续审核: {str(e)}"

# ================ 资讯巡检功能 - 日志优化版（集成图片尺寸检查） ================

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
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 解析返回的信息
            news_info = parse_news_info(assistant_message)
            return news_info
            
        except Exception as e:
            retry_count += 1
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
        
        return {'images': images, 'content': content}
        
    except Exception as e:
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
    else:
        valid_content = raw_content.strip()
    
    # 验证文本质量
    if len(valid_content) < 20:
        return "文本提取失败"
    
    return valid_content

def process_news_file(filename, combined_api_key, session_id):
    """处理资讯巡检文件 - 日志优化版（集成图片尺寸检查）"""
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
        
        logger.info(f"[NEWS] 开始处理资讯文件 - 总数: {total_rows}")
        
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
                
                # 每10%记录一次进度日志
                if processed == 1 or processed % max(1, total_rows // 10) == 0 or processed == total_rows:
                    logger.info(f"[NEWS] 处理进度: {processed}/{total_rows} ({progress}%)")
                    update_task_status('news', session_id, progress=progress, processed=processed, 
                                     message='处理中: %d/%d (%d%%)' % (processed, total_rows, progress))
                else:
                    update_task_status('news', session_id, progress=progress, processed=processed)
                
                # 处理资讯内容
                news_url = row['资讯链接']
                
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
                
                # 每5条或最后一条保存一次结果
                if (index + 1) % 5 == 0 or (index + 1) == total_rows:
                    result_path = get_result_path('news', session_id)
                    df.to_excel(result_path, index=False)
                
            except Exception as e:
                logger.error(f"[NEWS] 处理项目 #{index+1} 错误: {str(e)}")
                handle_processing_error(df, index, session_id, str(e))
                continue
        
        # 保存最终结果
        result_path = get_result_path('news', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('news', session_id, status='done', progress=100, 
                         message='资讯巡检完成')
        
        # 添加到历史记录
        add_to_history('news', session_id, os.path.basename(filename), total_rows, 
                      task_status['news'][session_id]['statistics'])
        
        logger.info(f"[NEWS] 任务完成 - 总数: {total_rows}")
        
    except Exception as e:
        logger.error(f"[NEWS] 处理错误: {str(e)}")
        update_task_status('news', session_id, status='error', 
                         message=f'处理出错: {str(e)}')
        # 即使出错也记录历史
        add_to_history('news', session_id, os.path.basename(filename), 
                      len(df) if 'df' in locals() else 0, 
                      task_status['news'][session_id]['statistics'])

def process_news_item_complete(news_url, api_key_info_extract, api_key_image_audit, 
                              api_key_text_audit, session_id):
    """处理单条资讯内容 - 日志优化版（集成图片尺寸检查）"""
    all_results = []
    all_tags = []
    image_count = 0
    text_audit_result = '未审核'
    text_audit_tags = []
    skipped_small_images = 0  # 新增：记录跳过的图片数量
    image_audit_details = []  # 新增：存储图片审核详情
    
    # 步骤1: 使用信息读取Agent同时提取图片链接和文本内容
    update_task_status('news', session_id, message=f'正在读取资讯信息...')
    news_info = extract_news_info(news_url, api_key_info_extract)
    
    # 解析返回的信息
    image_urls = news_info.get('images', [])
    raw_text_content = news_info.get('content', '')
    
    # 步骤2: 截取有效文本内容
    text_content = extract_valid_content(raw_text_content)
    
    # 步骤3: 审核所有图片（增加尺寸检查）
    if image_urls:
        logger.info(f"[NEWS] 提取到 {len(image_urls)} 张图片，开始审核...")
        for i, image_url in enumerate(image_urls):
            try:
                # 新增：检查图片尺寸
                should_audit, size_info = check_image_size(image_url)
                
                if not should_audit:
                    # 图片尺寸过小，跳过审核
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
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"[NEWS] 图片 {i+1} 处理失败: {str(e)}")
                # 处理失败的图片也记录下来
                all_results.append('处理失败')
                all_tags.append('图片处理失败')
                continue
    else:
        all_results.append('无图片')
        all_tags.append('无图片')
    
    # 步骤4: 审核文本内容
    if text_content and text_content != "文本提取失败":
        try:
            update_task_status('news', session_id, message='正在审核文本内容...')
            text_audit_result, text_audit_tags = audit_news_text_fixed(text_content, api_key_text_audit)
            all_results.append(text_audit_result)
            all_tags.extend(text_audit_tags)
        except Exception as e:
            logger.error(f"[NEWS] 文本审核失败: {str(e)}")
            all_results.append('处理失败')
            all_tags.append('文本审核失败')
    else:
        all_results.append('文本提取失败')
        all_tags.append('文本提取失败')
    
    # 汇总结果
    final_result, final_tags = aggregate_news_results(all_results, all_tags)
    
    return {
        'final_result': final_result,
        'final_tags': final_tags,
        'image_count': len(image_urls),
        'skipped_small_images': skipped_small_images,
        'image_results': image_audit_details,
        'text_result': text_audit_result,
        'text_tags': text_audit_tags
    }

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
            
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=(10, 3000)
            )
            
            if response.status_code != 200:
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    time.sleep(2)
                    continue
                response.raise_for_status()
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 使用统一的审核结果解析
            result, tags = parse_audit_result(assistant_message)
            return result, tags
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', ['图片审核失败']
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
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
            
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 使用统一的审核结果解析
            result, tags = parse_audit_result(assistant_message)
            return result, tags
            
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', ['文本审核失败']
            time.sleep(2)
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                return '处理失败', ['文本审核失败']
            time.sleep(2)
    
    return '处理失败', ['文本审核失败']

def aggregate_news_results(all_results, all_tags):
    """汇总审核结果 - 过滤掉小图片标签"""
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
                     message=f'资讯 #{index+1} 处理异常，继续处理下一项')
    
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