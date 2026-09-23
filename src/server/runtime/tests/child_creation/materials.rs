use super::*;
mod transport;
use crate::backend::{
    NativeAgentRequest, NativeMaterialOperation, NativeMaterialRequest, PromptImage,
};
use crate::domain_transcript::{EntryKind, EntryStatus};
use nakode_protocol::{MaterialScope, MaterialSource, QueryResult};

async fn child(h: &mut Harness) -> SessionId {
    let command = creation(&h.runtime, &h.parent);
    send(&mut h.runtime, "material-child", command, false)
        .await
        .unwrap()
        .resource_id
        .unwrap()
        .into()
}

fn scope(parent: &SessionId, child: &SessionId) -> MaterialScope {
    MaterialScope {
        parent_session_id: parent.clone(),
        source: MaterialSource {
            session_id: child.clone(),
            run_id: None,
        },
    }
}

fn images(h: &mut Harness, child: &SessionId, count: usize) {
    let state = h.runtime.core.engine_for_mut(child).unwrap().state_mut();
    for index in 0..count {
        let key = format!("image-{index}");
        state.transcript.set_labeled_images(
            &key,
            vec![(
                format!("evidence-{index}.png"),
                PromptImage {
                    mime_type: "image/png".to_owned(),
                    data: crate::image_handoff::tests::png(64, 32),
                },
            )],
        );
        state.transcript.upsert(
            &key,
            EntryKind::Assistant,
            "ASSISTANT",
            "Evidence",
            EntryStatus::Complete,
        );
    }
}

async fn query(h: &mut Harness, query: Query) -> Result<QueryResult, ServiceError> {
    let endpoint = h.runtime.endpoint.clone();
    let read = endpoint.execute_query(ClientId::from("parent-material-client"), query);
    let serve = async {
        let request = h.runtime.requests.recv().await.unwrap();
        h.runtime.handle_request(request).await;
    };
    let (result, ()) = tokio::join!(read, serve);
    result.map(|value| value.value)
}

#[tokio::test]
async fn child_materials_page_metadata_and_retrieve_only_the_selected_crop() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 3);
    let scope = scope(&h.parent, &child);
    let QueryResult::ChildMaterials(first) = query(
        &mut h,
        Query::ListChildMaterials {
            scope: scope.clone(),
            after: None,
            limit: 2,
        },
    )
    .await
    .unwrap() else {
        panic!("page")
    };
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.scope, scope);
    assert!(first.next_after.is_some());
    assert!(!serde_json::to_string(&first).unwrap().contains("\"data\""));
    let QueryResult::ChildMaterials(last) = query(
        &mut h,
        Query::ListChildMaterials {
            scope: scope.clone(),
            after: first.next_after,
            limit: 2,
        },
    )
    .await
    .unwrap() else {
        panic!("page")
    };
    assert_eq!(last.items.len(), 1);
    assert!(last.next_after.is_none());
    let reference = first.items[0].artifact_id.to_string();
    let QueryResult::ChildMaterial(crop) = query(
        &mut h,
        Query::GetChildMaterial {
            scope: scope.clone(),
            image_reference: reference.clone(),
            transform: Some(nakode_protocol::ImageTransform {
                max_width: Some(16),
                ..Default::default()
            }),
        },
    )
    .await
    .unwrap() else {
        panic!("image")
    };
    assert_eq!(crop.scope, scope);
    assert_eq!(
        (crop.artifact.width, crop.artifact.height),
        (Some(16), Some(8))
    );
    assert_eq!(
        crate::image_handoff::dimensions(&crop.artifact.data).unwrap(),
        (16, 8)
    );
    let QueryResult::ChildMaterial(original) = query(
        &mut h,
        Query::GetChildMaterial {
            scope,
            image_reference: reference,
            transform: None,
        },
    )
    .await
    .unwrap() else {
        panic!("original")
    };
    assert_eq!(
        original.artifact.data,
        crate::image_handoff::tests::png(64, 32)
    );
    assert!(h.runtime.effects.backends.session_commands.is_empty());
}

#[tokio::test]
async fn child_materials_reauthorize_parent_profile_and_exact_artifact_source() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 1);
    let child_scope = scope(&h.parent, &child);
    let QueryResult::ChildMaterials(page) = query(
        &mut h,
        Query::ListChildMaterials {
            scope: child_scope.clone(),
            after: None,
            limit: 1,
        },
    )
    .await
    .unwrap() else {
        panic!("page")
    };
    let reference = page.items[0].artifact_id.to_string();
    let own_scope = scope(&h.parent, &h.parent);
    let denied = query(
        &mut h,
        Query::GetChildMaterial {
            scope: own_scope,
            image_reference: reference.clone(),
            transform: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code, ErrorCode::NotFound);
    let foreign = scope(&SessionId::from("unrelated-parent"), &child);
    assert_eq!(
        query(
            &mut h,
            Query::ListChildMaterials {
                scope: foreign,
                after: None,
                limit: 1
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::NotFound
    );
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection.execute("UPDATE session_skill_profiles SET profile_id = 'different-owner' WHERE session_id = ?1", [child.as_str()]).unwrap();
    assert_eq!(
        query(
            &mut h,
            Query::GetChildMaterial {
                scope: child_scope,
                image_reference: reference,
                transform: None
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn child_materials_bounds_and_missing_cursors_never_restart_or_cross_sources() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 1);
    let scope = scope(&h.parent, &child);
    for limit in [0, 65, u32::MAX] {
        assert_eq!(
            query(
                &mut h,
                Query::ListChildMaterials {
                    scope: scope.clone(),
                    after: None,
                    limit
                }
            )
            .await
            .unwrap_err()
            .code,
            ErrorCode::InvalidRequest
        );
    }
    assert_eq!(
        query(
            &mut h,
            Query::ListChildMaterials {
                scope: scope.clone(),
                after: Some("missing".into()),
                limit: 1
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::NotFound
    );
    assert_eq!(
        query(
            &mut h,
            Query::GetChildMaterial {
                scope: scope.clone(),
                image_reference: "/foreign/path.png".to_owned(),
                transform: None
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::NotFound
    );
    let state = h.runtime.core.engine_for_mut(&child).unwrap().state_mut();
    state.transcript.set_labeled_images(
        "too-large",
        vec![(
            "large.png".into(),
            PromptImage {
                mime_type: "image/png".into(),
                data: vec![0; 5 * 1024 * 1024 + 1],
            },
        )],
    );
    state.transcript.upsert(
        "too-large",
        EntryKind::Assistant,
        "ASSISTANT",
        "",
        EntryStatus::Complete,
    );
    let QueryResult::ChildMaterials(page) = query(
        &mut h,
        Query::ListChildMaterials {
            scope: scope.clone(),
            after: None,
            limit: 64,
        },
    )
    .await
    .unwrap() else {
        panic!("page")
    };
    let reference = page.items.last().unwrap().artifact_id.to_string();
    assert_eq!(
        query(
            &mut h,
            Query::GetChildMaterial {
                scope,
                image_reference: reference,
                transform: None
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::InvalidRequest
    );
}

#[tokio::test]
async fn native_child_material_reads_use_the_runtime_without_any_cloud_client() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    images(&mut h, &child, 1);
    for requester in [None, Some("unrelated-run".to_owned())] {
        let (respond, response) = tokio::sync::oneshot::channel();
        h.runtime
            .handle_native_agent_request(NativeAgentRequest::Material(NativeMaterialRequest {
                owner_session_id: h.parent.to_string(),
                requester_run_id: requester.clone(),
                source: MaterialSource {
                    session_id: child.clone(),
                    run_id: None,
                },
                operation: NativeMaterialOperation::List {
                    after: None,
                    limit: 1,
                },
                respond,
            }))
            .await;
        let result = response.await.unwrap();
        if requester.is_some() {
            assert!(result.unwrap_err().contains("outside this delegated run"));
        } else {
            assert!(
                matches!(result.unwrap(), QueryResult::ChildMaterials(page) if page.items.len() == 1)
            );
        }
    }
}

fn native_run_with_image(h: &mut Harness, child: &SessionId) -> String {
    let directory = h.directory.path().join("agents");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("reviewer.toml"),
        "slug = 'reviewer'\ndescription = 'Review evidence'\nsystem_prompt = 'Review only'\nfirst_message = 'Reviewing'\nmodel = 'openai-codex/gpt-5'\nenabled = true\n",
    ).unwrap();
    let (accepted, _) = h
        .runtime
        .core
        .delegate_command(
            child,
            "reviewer",
            "Visual review",
            "Inspect evidence",
            None,
            &[],
        )
        .unwrap();
    let run = accepted.resource_id.unwrap();
    let returned = crate::runtime::ReturnedImage {
        id: "native-review-image".into(),
        turn_id: "review-turn".into(),
        provider_id: CODEX_PROVIDER.into(),
        model_id: "gpt-5".into(),
        history_index: 0,
        sequence: 0,
        attachment: crate::backend::PromptAttachment {
            label: "native.png".into(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".into(),
                data: crate::image_handoff::tests::png(64, 32),
            }),
        },
    };
    let state = h.runtime.core.engine_for_mut(child).unwrap().state_mut();
    // Test the normalized persistence boundary without starting a provider.
    state.session_id = Some(child.to_string());
    let effects = state.handle_subagent_backend(&run, BackendEvent::ImageReturned(returned));
    let record = effects
        .into_iter()
        .find_map(|effect| match effect {
            Effect::PersistSubagent(record) => Some(record),
            _ => None,
        })
        .expect("returned image must be a persistence boundary");
    assert_eq!(record.images.len(), 1);
    h.runtime
        .effects
        .persistence
        .sessions
        .save_subagent(&record)
        .unwrap();
    run
}

#[tokio::test]
async fn native_run_materials_require_the_exact_owning_session_and_run() {
    let mut h = harness().await;
    let child = child(&mut h).await;
    let run = native_run_with_image(&mut h, &child);
    let mut source = scope(&h.parent, &child);
    source.source.run_id = Some(run.clone().into());
    let QueryResult::ChildMaterials(page) = query(
        &mut h,
        Query::ListChildMaterials {
            scope: source.clone(),
            after: None,
            limit: 2,
        },
    )
    .await
    .unwrap() else {
        panic!("run page")
    };
    assert_eq!(page.run_title.as_deref(), Some("Visual review"));
    assert_eq!(page.items.len(), 1);
    let reference = page.items[0].artifact_id.to_string();
    let QueryResult::ChildMaterial(image) = query(
        &mut h,
        Query::GetChildMaterial {
            scope: source.clone(),
            image_reference: reference.clone(),
            transform: None,
        },
    )
    .await
    .unwrap() else {
        panic!("run image")
    };
    assert_eq!(
        image.artifact.data,
        crate::image_handoff::tests::png(64, 32)
    );
    let (retained, _handle) = super::super::retained_history_runtime(h.directory.path()).await;
    let QueryResult::ChildMaterial(restored) = retained
        .read_child_materials(Query::GetChildMaterial {
            scope: source.clone(),
            image_reference: reference.clone(),
            transform: None,
        })
        .unwrap()
    else {
        panic!("retained run image")
    };
    assert_eq!(restored.artifact.data, image.artifact.data);
    assert!(
        retained.core.engine_for(&child).is_none(),
        "inspection must not reopen a provider"
    );
    // Neither the child primary transcript nor the parent's run tree contains this image.
    source.source.run_id = None;
    assert!(
        query(
            &mut h,
            Query::GetChildMaterial {
                scope: source.clone(),
                image_reference: reference.clone(),
                transform: None
            }
        )
        .await
        .is_err()
    );
    source.source.session_id = h.parent.clone();
    source.source.run_id = Some(run.into());
    assert!(
        query(
            &mut h,
            Query::GetChildMaterial {
                scope: source,
                image_reference: reference,
                transform: None
            }
        )
        .await
        .is_err()
    );
}
