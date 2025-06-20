import os
import numpy as np
import torch
import torch.optim as optim
import torch.nn.functional as F
import pandas as pd
from datasets import Dataset, DatasetDict
from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, average_precision_score
import wandb
from transformers import TrainingArguments, Trainer
import optuna   #기계학습 모델의 하이퍼파라미터 자동 조정하고 최적화하는 오픈 소스 라이브러리

#토크나이저 관련 경고 무시
os.environ['TOKENIZERS-PARALLELISM'] = 'true'

#device 지정: 딥러닝 학습 속도 향상
device = torch.device('cuda:1' if torch.cuda.is_available() else 'cpu')
#print(f'사용 디바이스: {device}')   #cuda:1

df = pd.read_excel('real_matjib/dataset/instagram_tags_labeling.xlsx')

#결측치 없애고 데이터타입 str로
df = df.fillna('').astype(str)

df['reviews'] = (
    #사전모델의 max_lengths가 512여서 가장 긴 df['text']는 마지막에 추가
    df['ids'] + ' ' +   #계정 아이디
    df['tags'] + ' ' +  #태그
    df['cmts'] + ' ' +  #댓글
    df['text']  #본문
)

#컬럼명 수정
df = df.rename(columns={'category':'label'})

#깔끔하게 보이기 위해 내부에 리스트 문자열 제거
df['reviews'] = df['reviews'].str.replace(r'[\[\]]', '', regex=True)

#라벨링 된 행만 필터링
df_reviews = df[['food_house', 'search', 'reviews', 'label']].loc[:1001].copy()

#라벨링 학습위해서는 라벨링 값 float 타입으로 전환
df_reviews['label'] = df_reviews['label'].map({'일반':0, '홍보':1}).astype(float)

ds = Dataset.from_pandas(df_reviews)
split_ds = ds.train_test_split(test_size=0.2, seed=42)
dataset = DatasetDict({
    'train': split_ds['train'],
    'test': split_ds['test']
})

tokenizer = AutoTokenizer.from_pretrained('klue/roberta-base')

def preprocess_function(examples):
    return tokenizer(
        examples['reviews'],
        truncation = True,
        padding = 'max_length',
        max_length = 512
    )

tokenized_datasets = {
    'train': dataset['train'].map(preprocess_function, batched=True),
    'validation': dataset['test'].map(preprocess_function, batched=True)
}

def model_init():
    return AutoModelForSequenceClassification.from_pretrained(
        'klue/roberta-base',
        num_labels = 1
    )

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    #sigmoid로 확률값 변환(이진분류)
    probs = 1/(1+np.exp(-logits))
    predictions = (probs > 0.5).astype(int)

    metrics = {
        'eval_accuracy': accuracy_score(labels, predictions),
        'eval_f1': f1_score(labels, predictions),
        'eval_precision': precision_score(labels, predictions),
        'eval_recall': recall_score(labels, predictions)
    }

    try:
        metrics['eval_roc_auc'] = roc_auc_score(labels, probs)
        metrics['eval_pr_auc'] = average_precision_score(labels, probs)
    except ValueError:
        metrics['eval_roc_auc'] = float('nan')
        metrics['eval_pr_auc'] = float('nan')

    print("compute_metrics 반환값:", metrics)  # 디버깅용
    return metrics

def objective(trial):
    #하이퍼파라미터 탐색
    params = {
        'learning_rate' : trial.suggest_float('learning_rate', 1e-5, 5e-5, log=True),
        #'batch_size' : trial.suggest_categorical('batch_size', [8]),
        'train_batch_size' : trial.suggest_categorical('train_batch_size', [8, 16, 32]),
        'eval_batch_size' : trial.suggest_categorical('eval_batch_size', [16, 32, 64]),
        'num_train_epochs' : trial.suggest_int('num_train_epochs', 3, 10),
        'weight_decay' : trial.suggest_float('weight_decay', 0.001, 0.01, log=True)
    }

    #wandb run 생성
    run = wandb.init(
        project='hugging_face_with_Optuna',
        #experiment-1 부터
        name=f'experiment-{trial.number + 1}',
        config=params,
        reinit=True    #앞으로 최근 wandb 버전에서는 reinit -> return_previous or finish_previous
    )

    print("wandb.run:", wandb.run)  # 디버깅용

    training_args = TrainingArguments(
        output_dir='/.results/exp{trial.number}',
        #experiment-1 부터
        run_name=f'experiment-{trial.number + 1}',
        learning_rate=params['learning_rate'],
        per_device_train_batch_size=params['train_batch_size'],
        per_device_eval_batch_size=params['eval_batch_size'],
        num_train_epochs=params['num_train_epochs'],
        weight_decay=params['weight_decay'],
        eval_strategy='epoch',
        save_strategy='epoch',
        load_best_model_at_end=True,
        report_to='wandb'   #wandb 연동
    )

    trainer = Trainer(
        model=model_init(),
        args=training_args,
        train_dataset=tokenized_datasets['train'],
        eval_dataset=tokenized_datasets['validation'],
        compute_metrics=compute_metrics,
        tokenizer=tokenizer ,
    )

    trainer.train()

    metrics = trainer.evaluate()
    print("trainer.evaluate() 결과:", metrics)  # 디버깅용

    #모델 성능 평가
    pred_output = trainer.predict(tokenized_datasets['validation'])
    y_true = pred_output.label_ids
    logits = pred_output.predictions
    probs = 1/(1+np.exp(-logits))
    probs = probs.reshape(-1)
    probs_for_wandb = np.stack([1-probs, probs], axis=1)

    #threshold에 따른 roc-auc와 pr-auc 시각화
    wandb.log({'ROC AUC': wandb.plot.roc_curve(y_true, probs_for_wandb)})
    wandb.log({'PR AUC': wandb.plot.pr_curve(y_true, probs_for_wandb)})

    run.finish()

    return metrics['eval_accuracy'] #Optuna는 하나의 지표만 반환

study = optuna.create_study(direction='maximize')
study.optimize(objective, n_trials=50)   #하이퍼파라미터 새로운 조합 시도 횟수(experiment 횟수)
print('Best params: ', study.best_params)
#Best params:  {'learning_rate': 4.032897223604241e-05, 'train_batch_size': 8, 'eval_batch_size': 32, 'num_train_epochs': 8, 'weight_decay': 0.0018773593172162295}

