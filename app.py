"""
AI智能内容巡检系统 - 修复版

支持评论审核和封面审核功能

主应用模块，处理Web请求和任务管理

修复内容：
1. 修复process_comment函数的API结果解析问题
2. 支持多种API返回格式（带编号和不带编号）
3. 过滤think标签内容，只解析实际结果
4. 增加备用解析策略和错误处理机制
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
from datetime import datetime
from flask import Flask, request, jsonify, render_template, send_file
from werkzeug.utils import secure_filename

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
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
    'cover': {}
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
    return os.path.join(UPLOAD_FOLDER, f"{audit_type}_{task_id}.xlsx")

def get_result_path(audit_type, session_id):
    """获取结果文件路径 - 使用传入的session_id而非Flask session"""
    task_id = get_task_id(audit_type, session_id)
    return os.path.join(RESULT_FOLDER, f"{audit_type}_{task_id}_result.xlsx")

def add_to_history(audit_type, session_id):
    """添加到历史记录 - 使用传入的session_id而非Flask session"""
    task_id = get_task_id(audit_type, session_id)
    history_records.append({
        'id': task_id,
        'audit_type': audit_type,
        'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'result_path': get_result_path(audit_type, session_id)
    })

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
        
        if audit_type not in ['comment', 'cover']:
            return jsonify({'error': '无效的审核类型'}), 400
        
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
        logger.error(f"文件上传错误: {str(e)}")
        return jsonify({'error': f'文件上传失败: {str(e)}'}), 500

@app.route('/run', methods=['POST'])
def run_task():
    """启动任务"""
    try:
        # 获取参数
        data = request.get_json()
        audit_type = data.get('audit_type')
        api_key = data.get('api_key')
        session_id = data.get('session_id')
        
        if audit_type not in ['comment', 'cover']:
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
        else:
            thread = threading.Thread(target=process_cover_file, args=(filename, api_key, session_id))
        
        thread.daemon = True
        thread.start()
        
        return jsonify({'message': '任务已启动'})
        
    except Exception as e:
        logger.error(f"启动任务错误: {str(e)}")
        return jsonify({'error': f'启动任务失败: {str(e)}'}), 500

@app.route('/status/<audit_type>')
def get_status(audit_type):
    """获取任务状态"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover']:
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
        logger.error(f"获取状态错误: {str(e)}")
        return jsonify({'error': f'获取状态失败: {str(e)}'}), 500

@app.route('/control', methods=['POST'])
def control_task():
    """控制任务（暂停/继续/完成/结束）"""
    try:
        # 获取参数
        data = request.get_json()
        audit_type = data.get('audit_type')
        action = data.get('action')
        session_id = data.get('session_id')
        
        if audit_type not in ['comment', 'cover']:
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
        logger.error(f"控制任务错误: {str(e)}")
        return jsonify({'error': f'控制任务失败: {str(e)}'}), 500

@app.route('/statistics/<audit_type>')
def get_statistics(audit_type):
    """获取统计数据"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover']:
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
        logger.error(f"获取统计数据错误: {str(e)}")
        return jsonify({'error': f'获取统计数据失败: {str(e)}'}), 500

@app.route('/download/<audit_type>')
def download_result(audit_type):
    """下载结果文件"""
    try:
        session_id = request.args.get('session_id')
        
        if audit_type not in ['comment', 'cover']:
            return jsonify({'error': '无效的审核类型'}), 400
        
        # 获取结果文件路径
        result_path = get_result_path(audit_type, session_id)
        
        if not os.path.exists(result_path):
            return jsonify({'error': '结果文件不存在'}), 404
        
        # 返回文件
        return send_file(result_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"下载结果错误: {str(e)}")
        return jsonify({'error': f'下载结果失败: {str(e)}'}), 500

@app.route('/history')
def get_history():
    """获取历史记录"""
    try:
        return jsonify({'history': history_records})
        
    except Exception as e:
        logger.error(f"获取历史记录错误: {str(e)}")
        return jsonify({'error': f'获取历史记录失败: {str(e)}'}), 500

@app.route('/history/download/<history_id>')
def download_history(history_id):
    """下载历史结果文件"""
    try:
        # 查找历史记录
        for record in history_records:
            if record['id'] == history_id:
                result_path = record['result_path']
                
                if not os.path.exists(result_path):
                    return jsonify({'error': '历史结果文件不存在'}), 404
                
                # 返回文件
                return send_file(result_path, as_attachment=True)
        
        return jsonify({'error': '历史记录不存在'}), 404
        
    except Exception as e:
        logger.error(f"下载历史结果错误: {str(e)}")
        return jsonify({'error': f'下载历史结果失败: {str(e)}'}), 500

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
        update_task_status('comment', session_id, total=total_rows, message=f'数据准备完成，开始处理 {total_rows} 条评论')
        
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
                update_task_status('comment', session_id, progress=progress, processed=processed, message=f'开始处理评论 #{index+1}/{total_rows}')
                
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
                
                # 每条处理完成后保存一次结果，确保不丢失进度
                result_path = get_result_path('comment', session_id)
                df.to_excel(result_path, index=False)
                
                # 添加处理完成日志
                update_task_status('comment', session_id, message=f'评论 #{index+1}/{total_rows} 处理完成，结果: {result}', status='processing')
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"评论处理错误: {str(e)}")
                update_task_status('comment', session_id, message=f'评论 #{index+1} 处理异常: {str(e)}，继续处理下一条', status='warning')
                
                # 更新结果为处理失败
                df.at[index, '审核结果'] = '处理失败'
                df.at[index, '违规标签'] = '/'
                df.at[index, '审核时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 更新统计
                update_statistics('comment', session_id, '处理失败', [])
                
                # 保存当前结果
                result_path = get_result_path('comment', session_id)
                df.to_excel(result_path, index=False)
                
                # 继续处理下一条，不中断循环
                continue
        
        # 保存最终结果
        result_path = get_result_path('comment', session_id)
        df.to_excel(result_path, index=False)
        
        # 更新任务状态
        update_task_status('comment', session_id, status='done', progress=100, message='评论审核完成，请点击完成按钮')
        
        # 添加到历史记录
        add_to_history('comment', session_id)
        
    except Exception as e:
        logger.error(f"评论处理错误: {str(e)}")
        update_task_status('comment', session_id, status='error', message=f'处理出错: {str(e)}')

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
        update_task_status('cover', session_id, total=total_rows, message=f'数据准备完成，开始处理 {total_rows} 条封面链接')
        
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
                update_task_status('cover', session_id, progress=progress, processed=processed, message=f'开始处理项目 #{index+1}/{total_rows}')
                
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
                update_task_status('cover', session_id, message=f'项目 #{index+1}/{total_rows} 处理完成，结果: {result}', status='processing')
                
                # 添加间隔，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"封面处理项目 #{index} 错误: {str(e)}")
                update_task_status('cover', session_id, message=f'项目 #{index+1} 处理异常: {str(e)}，继续处理下一项', status='warning')
                
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
        
        # 添加到历史记录
        add_to_history('cover', session_id)
        
    except Exception as e:
        logger.error(f"封面处理错误: {str(e)}")
        update_task_status('cover', session_id, status='error', message=f'处理出错: {str(e)}')

def process_comment(comment, api_key):
    """处理单条评论 - 修复版本，解决API结果解析问题
    
    修复内容：
    1. 支持多种API返回格式（带编号和不带编号）
    2. 过滤think标签内容，只解析实际结果
    3. 增加备用解析策略
    4. 增强错误处理和重试机制
    """
    # 最大重试次数
    max_retries = 3
    retry_count = 0
    # 设置超时时间（连接超时+读取超时）
    api_timeout = (10, 3000)  # (连接超时10秒, 读取超时3000秒)
    
    while retry_count < max_retries:
        try:
            # 构建请求数据
            data = {
                "query": f"请审核以下评论内容是否低质，并给出审核结果和低质标签：\n\n{comment}",
                "inputs": {},
                "response_mode": "blocking",
                "user": "audit_system"
            }
            
            # 发送请求
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            logger.info(f"评论审核请求数据: {json.dumps(data)}")
            
            # 发送请求，添加3000秒超时机制
            response = requests.post(
                API_URL, 
                headers=headers, 
                json=data, 
                timeout=api_timeout
            )
            
            logger.info(f"评论审核响应状态: {response.status_code}")
            
            # 处理非200状态码
            if response.status_code != 200:
                logger.error(f"评论审核响应错误: {response.text}")
                # 特殊处理501错误
                if response.status_code == 501 and "conversation_id" in response.text:
                    retry_count += 1
                    logger.warning(f"501错误触发重试 ({retry_count}/{max_retries})")
                    time.sleep(2)
                    continue
                # 其他非200状态码直接引发异常
                response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            logger.info(f"评论审核原始响应: {assistant_message}")
            
            # 解析API返回结果
            result, tags = parse_audit_result(assistant_message)
            
            logger.info(f"评论审核解析结果: {result}, 标签: {tags}")
            return result, tags
            
        except requests.exceptions.Timeout as timeout_err:
            # 专门处理超时异常
            retry_count += 1
            timeout_type = "连接" if "connect" in str(timeout_err).lower() else "读取"
            logger.error(f"API请求超时 ({timeout_type}) (尝试 {retry_count}/{max_retries}): {str(timeout_err)}")
            
            if retry_count >= max_retries:
                logger.critical("API请求达到最大超时重试次数")
                return '处理失败', []
            
            # 指数退避策略
            sleep_time = 2 ** retry_count
            logger.info(f"将在 {sleep_time} 秒后重试...")
            time.sleep(sleep_time)
            
        except requests.exceptions.RequestException as req_err:
            # 处理其他网络请求异常
            retry_count += 1
            logger.error(f"网络请求异常 (尝试 {retry_count}/{max_retries}): {str(req_err)}")
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
            
        except Exception as e:
            # 处理其他所有异常
            retry_count += 1
            logger.error(f"未处理的异常 (尝试 {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            time.sleep(2)
    
    # 所有重试失败后的默认返回
    return '处理失败', []


def parse_audit_result(assistant_message):
    """解析审核结果 - 修复版本，支持多种格式
    
    支持的格式：
    1. 带编号格式：（1）审核结果：正常 （2）低质标签：/
    2. 不带编号格式：审核结果：正常 低质标签：/
    3. 混合格式和各种变体
    
    Args:
        assistant_message (str): API返回的完整消息
        
    Returns:
        tuple: (审核结果, 标签列表)
    """
    # 初始化默认值
    result = "处理失败"
    tags = []
    
    try:
        # 第一步：过滤think标签内容，只保留实际结果部分
        # 移除<think>...</think>标签及其内容
        think_pattern = r'<think>.*?</think>'
        filtered_message = re.sub(think_pattern, '', assistant_message, flags=re.DOTALL)
        
        # 清理多余的空白字符
        filtered_message = filtered_message.strip()
        
        logger.info(f"过滤think标签后的内容: {filtered_message}")
        
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
                    logger.info(f"使用模式{i+1}成功解析结果: {result}")
            
            if not tag_found:
                tag_match = re.search(tag_pattern, filtered_message, re.IGNORECASE)
                if tag_match:
                    tag_str = tag_match.group(1).strip()
                    tags = parse_tags(tag_str)
                    tag_found = True
                    logger.info(f"使用模式{i+1}成功解析标签: {tags}")
        
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
        
        logger.info(f"最终解析结果: 结果={result}, 标签={tags}")
        
    except Exception as e:
        logger.error(f"解析审核结果时发生异常: {str(e)}")
        result = "处理失败"
        tags = []
    
    return result, tags


def parse_tags(tag_str):
    """解析标签字符串，返回标签列表
    
    Args:
        tag_str (str): 标签字符串
        
    Returns:
        list: 标签列表
    """
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
    """从内容中提取可能的违规标签
    
    Args:
        content (str): 内容文本
        
    Returns:
        list: 提取的标签列表
    """
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
    update_task_status('cover', session_id, message=f'项目 #{index+1} 应用速率限制...')
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
                "Authorization": f"Bearer {api_key}"  # 保持Bearer和密钥之间的空格
            }
            
            # 打印请求数据用于调试
            logger.info(f"封面审核请求数据: {json.dumps(data)}")
            
            # 记录开始时间
            start_time = time.time()
            
            # 发送请求
            update_task_status('cover', session_id, message=f'项目 #{index+1} 发送请求 (尝试 {retry_count+1}/{max_retries})...')
            response = requests.post(API_URL, headers=headers, json=data)
            
            # 打印响应状态和内容用于调试
            logger.info(f"封面审核响应状态: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"封面审核响应错误: {response.text}")
                
                # 特殊处理501错误（对话ID不存在）
                if response.status_code == 501 and "conversation_id" in response.text:
                    # 重新尝试，不使用conversation_id
                    retry_count += 1
                    logger.info(f"封面审核重试 {retry_count}/{max_retries}")
                    time.sleep(2)  # 等待2秒后重试
                    continue
            
            response.raise_for_status()
            
            # 解析响应
            result_data = response.json()
            assistant_message = result_data.get('answer', '')
            
            # 保存conversation_id以便后续使用
            conversation_id = result_data.get('conversation_id', '')
            logger.info(f"获取到conversation_id: {conversation_id}")
            
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
            logger.info(f"封面审核结果: {result}, 标签: {tags}")
            
            # 记录处理时间
            process_time = time.time() - start_time
            logger.info(f"封面审核处理时间: {process_time:.2f}秒")
            
            return result, tags
            
        except Exception as e:
            retry_count += 1
            logger.error(f"封面处理API错误 (尝试 {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                return '处理失败', []
            
            # 等待后重试
            time.sleep(2)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

